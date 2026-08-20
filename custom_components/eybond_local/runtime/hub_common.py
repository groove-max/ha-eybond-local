"""Shared types, constants and pure helpers for the runtime hub family."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from time import monotonic, time as _wall_time
from typing import Any, Callable

from ..canonical_telemetry import (
    apply_canonical_measurements,
    canonical_measurements_for_driver,
    project_canonical_telemetry,
)
from ..const import (
    CONNECTION_TYPE_EYBOND,
    DEFAULT_DRIVER_DETECTION_STRATEGY,
    DRIVER_DETECTION_FULL_SCAN,
    DRIVER_HINT_AUTO,
)
from ..connection.models import EybondConnectionSpec
from ..connection.session_handle import ADAPTER_COLLECTOR_AT_COMMANDS
from ..collector_identity import reconcile_durable_pn
from ..collector.capabilities import (
    collector_capability_profile_from_runtime,
    parse_esp_collector_hardware_token,
)
from ..collector.cloud_family import (
    apply_collector_cloud_family_observation,
    collector_cloud_family_observation_from_collector,
    collector_cloud_family_observation_from_mapping,
    select_preferred_collector_cloud_family,
)
from ..collector_endpoint import (
    DEFAULT_COLLECTOR_SERVER_PORT,
    inspect_collector_server_endpoint,
    normalize_collector_server_endpoint as normalize_runtime_collector_server_endpoint,
)
from ..collector.management import (
    CollectorEndpointWriteResult,
    CollectorManagementCapabilities,
    CollectorManagementError,
    CollectorManagementUnsupportedError,
    CollectorSystemActionResult,
    select_collector_management_adapter,
)
from ..drivers.base import InverterDriver
from ..drivers.read_result import (
    DriverReadMode,
    DriverReadResult,
    coerce_driver_read_result,
)
from ..drivers.command_support import (
    clear_unsupported_commands,
    seed_unsupported_commands,
)
from ..drivers.registry import iter_drivers
from .driver_detection import (
    DetectedDriverContext,
    DriverSweepNoMatch,
    async_detect_inverter,
    async_detect_inverter_candidates,
)
from .link_baud_sweep import (
    RuntimeLinkBaudChannel,
    async_run_link_baud_sweep,
    catalog_link_baud_hints,
    default_runtime_driver_sweep_seconds,
    driver_keys_for_link_baud,
    parse_reported_baud,
)
from ..link_models import EybondLinkRoute
from ..link_transport import async_send_payload, select_payload_route
from ..models import CapabilityBlocker, DetectedInverter, RuntimeSnapshot, WriteCapability
from ..metadata.compiled_detection_catalog import (
    PROBE_ACTION_MODBUS_READ,
    load_compiled_detection_catalog,
)
from ..telemetry import TypedTelemetryFrame, fold_driver_telemetry
from ..payload.modbus import ModbusSession, to_signed_16
from ..runtime_labels import runtime_path_label
from ..support.shadow_learning import ShadowWriteObservation
from .collector_metadata import (
    CollectorMetadataRefreshResult,
    CollectorMetadataService,
)
from .link import EybondRuntimeLinkManager, resolve_server_ip
from .manager import RuntimeInverterCandidate

logger = logging.getLogger(__name__)

# A same-identity TCP replacement is allowed one bounded recovery attempt using
# the existing runtime reconnect budget. This is not a polling/NAT timer: the
# longer wait is enabled only by an observed owned-session generation change.
_SESSION_HANDOVER_CONNECT_TIMEOUT = 5.0
_SESSION_HANDOVER_MAX_GENERATIONS = 3


def _split_collector_endpoint(endpoint: object) -> tuple[str, int | None, str]:
    raw = str(endpoint or "").strip()
    if not raw:
        return "", None, ""
    try:
        parsed = inspect_collector_server_endpoint(
            raw,
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return raw, None, ""
    return parsed.host, parsed.port, parsed.protocol


_DEFAULT_PROXY_CAPTURE_PORT = DEFAULT_COLLECTOR_SERVER_PORT
RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE = "collector_offline"
RUNTIME_DRIVER_STATE_DRIVER_UNBOUND = "driver_unbound"

# Bounded per-command timeout for the at_text support-archive ASCII probe;
# generous enough for a slow 2400-baud line response, small enough that the
# driver-provided probe commands stay under half a minute even in total silence.
_AT_TEXT_ASCII_PROBE_TIMEOUT = 3.0
RUNTIME_DRIVER_STATE_DRIVER_BOUND = "driver_bound"

# --- Explicit runtime state-machine tracks -----------------------------------
# These are diagnostic/auditable projections of the hub's otherwise-implicit
# runtime state. They are derived in _build_snapshot and never drive transport
# or ownership decisions. The five tracks are kept INDEPENDENT so a collector-
# only observation can never erase a confirmed inverter identity: the inverter
# track has its own lifecycle (absent -> detecting -> provisional/live_confirmed,
# or conflict) that is not coupled to the session/collector tracks.
RUNTIME_SESSION_STATE_OFFLINE = "offline"
RUNTIME_SESSION_STATE_ONLINE = "online"

RUNTIME_COLLECTOR_STATE_UNKNOWN = "unknown"
RUNTIME_COLLECTOR_STATE_IDENTIFIED = "identified"

RUNTIME_INVERTER_STATE_ABSENT = "absent"
RUNTIME_INVERTER_STATE_DETECTING = "detecting"
RUNTIME_INVERTER_STATE_PROVISIONAL = "provisional"
RUNTIME_INVERTER_STATE_LIVE_CONFIRMED = "live_confirmed"
RUNTIME_INVERTER_STATE_CONFLICT = "conflict"
RUNTIME_INVERTER_STATE_AMBIGUOUS = "ambiguous"

RUNTIME_POLL_STATE_OFFLINE = "offline"
RUNTIME_POLL_STATE_DETECTING = "detecting"
RUNTIME_POLL_STATE_POLLING = "polling"
RUNTIME_POLL_STATE_DEGRADED = "degraded"

# A provisional (startup-persisted) binding refreshes itself against live
# detection. Bound the number of refresh attempts so a permanently-silent
# inverter cannot re-run detection on every single poll.
_INVERTER_BINDING_REFRESH_MAX_ATTEMPTS = 3
_PROVISIONAL_INVERTER_DETECTION_STATUSES = frozenset(
    {"startup_persisted_identity", "persisted_model_probe_degraded"}
)

# Keep a small ring of recent composite state transitions for the support
# package. Bounded on purpose: no unbounded growth, no per-poll logging.
_RUNTIME_STATE_TRANSITION_HISTORY_MAX = 20


def _inverter_identity_signature(inverter: object | None) -> str:
    """Return a stable identity signature (driver|model|serial) for comparison."""

    if inverter is None:
        return ""
    driver_key = str(getattr(inverter, "driver_key", "") or "").strip()
    model = str(getattr(inverter, "model_name", "") or "").strip()
    serial = str(getattr(inverter, "serial_number", "") or "").strip()
    return "|".join((driver_key, model, serial))


def _inverter_identity_is_present(inverter: object | None) -> bool:
    """Return whether an inverter object carries a usable model/serial identity."""

    if inverter is None:
        return False
    return bool(
        str(getattr(inverter, "model_name", "") or "").strip()
        or str(getattr(inverter, "serial_number", "") or "").strip()
    )


def _inverter_identities_conflict(current: object, candidate: object) -> bool:
    """Return whether two present identities denote different physical inverters.

    Serial number is the strongest signal: two non-empty different serials are a
    conflict. When a serial is unavailable, a different driver_key or model is a
    conflict. Same identity, or a refinement of a missing field, is NOT a conflict.
    """

    cur_serial = str(getattr(current, "serial_number", "") or "").strip()
    cand_serial = str(getattr(candidate, "serial_number", "") or "").strip()
    if cur_serial and cand_serial:
        return cur_serial != cand_serial
    cur_driver = str(getattr(current, "driver_key", "") or "").strip()
    cand_driver = str(getattr(candidate, "driver_key", "") or "").strip()
    if cur_driver and cand_driver and cur_driver != cand_driver:
        return True
    cur_model = str(getattr(current, "model_name", "") or "").strip()
    cand_model = str(getattr(candidate, "model_name", "") or "").strip()
    if cur_model and cand_model and cur_model != cand_model:
        return True
    return False


_VOLATILE_COLLECTOR_VALUE_KEYS: frozenset[str] = frozenset(
    {
        "smartess_collector_version",
        "smartess_protocol_raw_id",
        "smartess_protocol_asset_id",
        "smartess_protocol_asset_name",
        "smartess_protocol_suffix",
        "smartess_protocol_profile_key",
        "smartess_protocol_name",
        "smartess_device_address",
        "collector_protocol_version",
        "collector_type",
        "collector_hardware_version",
        "collector_local_ip_address",
        "collector_server_endpoint",
        "collector_callback_owner",
        "collector_reboot_required",
        "collector_transmission_mode",
        "collector_serial_baudrate",
        "collector_network_diagnostics",
        "collector_signal_strength",
        "collector_signal_strength_raw",
        "collector_signal_strength_source",
        "collector_signal_quality",
        "collector_upload_mode",
        "collector_system_time",
        "collector_link_status",
        "collector_cloud_heartbeat_value",
        "collector_ssid",
        "collector_wifi_scan_list",
        "collector_virtual_bridge",
        "collector_bridge_kind",
        "collector_bridge_version",
        "collector_udp_reply",
        "collector_udp_reply_from",
    }
)


def _is_home_assistant_callback_endpoint(
    endpoint: object,
    *,
    server_ip: str,
    advertised_server_ip: str,
    advertised_tcp_port: int,
) -> bool:
    host, port, protocol = _split_collector_endpoint(endpoint)
    normalized_host = host.lower()
    allowed_hosts = {
        str(server_ip or "").strip().lower(),
        str(advertised_server_ip or "").strip().lower(),
    }
    allowed_hosts.discard("")
    return (
        bool(normalized_host)
        and normalized_host in allowed_hosts
        and port in {int(advertised_tcp_port or 0), _DEFAULT_PROXY_CAPTURE_PORT}
        and protocol.upper() == "TCP"
    )


def _callback_owner_label(
    endpoint: object,
    *,
    server_ip: str,
    advertised_server_ip: str,
    advertised_tcp_port: int,
) -> str:
    host, _port, _protocol = _split_collector_endpoint(endpoint)
    normalized_host = host.lower()
    if _is_home_assistant_callback_endpoint(
        endpoint,
        server_ip=server_ip,
        advertised_server_ip=advertised_server_ip,
        advertised_tcp_port=advertised_tcp_port,
    ):
        return "Home Assistant"
    if "eybond" in normalized_host or "smartess" in normalized_host:
        return "SmartESS cloud"
    if normalized_host:
        return "Custom endpoint"
    return "Unknown"


def _collector_signal_quality(signal_strength: object) -> str:
    try:
        value = int(signal_strength)
    except (TypeError, ValueError):
        return "unknown"
    if value >= -70:
        return "excellent"
    if value >= -85:
        return "good"
    if value >= -100:
        return "fair"
    return "weak"


def _collector_signal_source_label(source: object) -> str:
    normalized = str(source or "").strip().lower()
    if normalized == "wifi_rssi":
        return "Wi-Fi RSSI"
    if normalized == "gprs_csq":
        return "GPRS CSQ"
    return "Unknown"


def _error_code(exc: BaseException) -> str:
    return str(exc)


def _is_retryable_collector_error(exc: BaseException) -> bool:
    """Return whether one transport error is worth retrying after reconnect."""

    return isinstance(exc, ConnectionError) and _error_code(exc) in {
        "collector_disconnected",
        "collector_not_connected",
        "collector_heartbeat_timeout",
        "collector_write_timeout",
    }


def _should_mark_snapshot_disconnected(exc: BaseException) -> bool:
    """Return whether one refresh error should make live sensors unavailable."""

    return _error_code(exc) in {
        "collector_disconnected",
        "collector_not_connected",
        "collector_heartbeat_timeout",
        "collector_write_timeout",
    }


def _should_force_reconnect(exc: BaseException) -> bool:
    """Return whether one refresh error warrants a forced collector reconnect."""

    return _error_code(exc) in {
        "collector_write_timeout",
    }


def _normalize_collector_server_endpoint(endpoint: str) -> str:
    return normalize_runtime_collector_server_endpoint(
        endpoint,
        require_explicit_port=False,
        require_explicit_protocol=False,
        preserve_shape=True,
    )




def _should_confirm_write(capability: WriteCapability) -> bool:
    """Return whether a write should be verified by immediate readback."""

    return capability.value_kind != "action"


def _write_readback_matches(
    capability: WriteCapability,
    *,
    requested_value: object,
    written_value: object,
    readback_value: object,
) -> bool:
    """Return whether one refreshed value confirms the requested write."""

    if readback_value == written_value or readback_value == requested_value:
        return True

    if capability.enum_value_map and isinstance(requested_value, int):
        expected_label = capability.enum_value_map.get(requested_value)
        if expected_label is not None and readback_value == expected_label:
            return True

    return False


def _write_not_confirmed_error(
    capability: WriteCapability,
    *,
    written_value: object,
    readback_value: object,
    refresh_error: str,
) -> RuntimeError:
    """Return one explicit error for a write that did not confirm by readback."""

    readback_text = "unavailable" if readback_value is None else repr(readback_value)
    message = (
        f"Command accepted, but {capability.display_name!r} did not confirm by readback. "
        f"Expected {written_value!r}, got {readback_text}."
    )
    if refresh_error:
        message = f"{message} Refresh reported {refresh_error}."
    return RuntimeError(f"write_not_confirmed:{capability.key}:{message}")


def _capture_ranges_from_schema(
    schema: Any,
    *,
    driver_key: str = "",
) -> tuple[tuple[int, int], ...]:
    """Build one generic support-capture plan including catalog identity reads."""

    planned: list[tuple[int, int]] = []
    for block_key in ("status", "serial", "live", "config"):
        try:
            block = schema.block(block_key)
        except KeyError:
            continue
        planned.append((block.start, block.count))

    try:
        planned.extend(
            (spec.register, spec.word_count)
            for spec in schema.spec_set("aux_config")
        )
    except KeyError:
        pass

    scalar_registers = getattr(schema, "scalar_registers", {})
    planned.extend(
        (register, 1) for register in sorted(set(scalar_registers.values()))
    )
    schema_ranges = _merge_capture_ranges(planned)
    identity_ranges = _catalog_identity_capture_ranges(driver_key)
    if not identity_ranges:
        return schema_ranges

    remaining_schema_ranges = tuple(
        item for item in schema_ranges if item not in identity_ranges
    )
    return tuple(dict.fromkeys((*identity_ranges, *remaining_schema_ranges)))


def _catalog_identity_capture_ranges(
    driver_key: object,
) -> tuple[tuple[int, int], ...]:
    """Return exact read-only Modbus action ranges for one compiled protocol."""

    if (
        type(driver_key) is not str
        or not driver_key
        or driver_key != driver_key.strip()
    ):
        return ()
    protocol = load_compiled_detection_catalog().protocols.get(driver_key)
    if protocol is None:
        return ()
    return tuple(
        dict.fromkeys(
            (action.register, action.count)
            for action in protocol.probe_actions
            if action.kind == PROBE_ACTION_MODBUS_READ
            and action.register is not None
            and action.count is not None
            and action.count > 0
        )
    )


def _merge_capture_ranges(
    ranges: list[tuple[int, int]] | tuple[tuple[int, int], ...],
) -> tuple[tuple[int, int], ...]:
    normalized = sorted(
        ((int(start), int(count)) for start, count in ranges if count > 0),
        key=lambda item: item[0],
    )
    if not normalized:
        return ()

    merged: list[tuple[int, int]] = []
    current_start, current_count = normalized[0]
    current_end = current_start + current_count

    for start, count in normalized[1:]:
        end = start + count
        if start <= current_end:
            current_end = max(current_end, end)
            current_count = current_end - current_start
            continue
        merged.append((current_start, current_count))
        current_start = start
        current_count = count
        current_end = end

    merged.append((current_start, current_count))
    return tuple(merged)


def _decode_ascii_words(registers: list[int]) -> str:
    chars: list[str] = []
    for value in registers:
        for byte in ((value >> 8) & 0xFF, value & 0xFF):
            if byte in (0x00, 0xFF):
                continue
            char = chr(byte)
            if char.isalnum() or char in " -_/.":
                chars.append(char)
    return "".join(chars)


def _format_support_range(start: int, values: list[int]) -> dict[str, Any]:
    entries = []
    for offset, value in enumerate(values):
        entries.append(
            {
                "register": start + offset,
                "u16": value,
                "s16": to_signed_16(value),
                "hex": f"0x{value:04X}",
            }
        )
    return {
        "start": start,
        "count": len(values),
        "ascii": _decode_ascii_words(values),
        "words": list(values),
        "values": entries,
    }
