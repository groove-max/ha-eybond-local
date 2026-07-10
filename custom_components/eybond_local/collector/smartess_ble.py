"""SmartESS BLE bootstrap helpers for local collector provisioning."""

from __future__ import annotations

import asyncio
import importlib.util
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from enum import Enum
import logging
import re
from typing import Any, Protocol


logger = logging.getLogger(__name__)


class SmartEssBleError(Exception):
    """Raised when one SmartESS BLE operation cannot proceed."""


@dataclass(frozen=True, slots=True)
class SmartEssBleHostCapability:
    """Result of one local BLE host capability probe."""

    available: bool
    backend: str
    reason: str = ""
    detail: str = ""


@dataclass(frozen=True, slots=True)
class SmartEssBleUuidLayout:
    """One known SmartESS collector BLE service layout."""

    name: str
    service_uuid: str
    write_uuid: str
    notify_uuid: str


@dataclass(frozen=True, slots=True)
class SmartEssBleScanRecord:
    """Decoded SmartESS-relevant fields from one BLE advertisement record."""

    local_name: str = ""
    local_pn: str = ""
    flags: int | None = None


@dataclass(frozen=True, slots=True)
class SmartEssBleCandidate:
    """One normalized SmartESS BLE discovery candidate."""

    address: str
    local_pn: str
    local_name: str
    device_name: str = ""
    service_uuids: tuple[str, ...] = ()
    device: object | None = field(default=None, repr=False, compare=False)

    @property
    def preferred_name(self) -> str:
        """Return the best user-facing name currently known for the candidate."""

        return self.local_name or self.device_name or self.local_pn


class SmartEssBleProvisionBranch(str, Enum):
    """Collector provisioning branch selected from the BLE AT version."""

    WFLKAP = "wflkap"
    INTPARA = "intpara"


class SmartEssBleProvisionOutcome(str, Enum):
    """Normalized provisioning outcomes across both SmartESS BLE branches."""

    SUCCESS = "success"
    DEGRADED = "degraded"
    FAILURE = "failure"


@dataclass(frozen=True, slots=True)
class SmartEssBleProvisioningInfo:
    """Version and branch data needed to run one provisioning attempt."""

    fw_version: str
    at_version: str
    branch: SmartEssBleProvisionBranch
    requires_restart: bool


@dataclass(frozen=True, slots=True)
class SmartEssBleWifiNetwork:
    """One Wi-Fi network exposed through `AT+INTPARA49?`."""

    ssid: str
    signal: int


@dataclass(frozen=True, slots=True)
class SmartEssBleProvisionResult:
    """Normalized result of one BLE Wi-Fi provisioning attempt."""

    branch: SmartEssBleProvisionBranch
    outcome: SmartEssBleProvisionOutcome
    status_code: str
    raw_response: str
    details: tuple[str, str, str] | None = None


_PN14_RE = re.compile(r"^[A-Z]\d{13}$")
_PN18_RE = re.compile(r"^[A-Z]\d{17}$")
_PN_SEARCH_RE = re.compile(r"[A-Z]\d{13}|[A-Z]\d{17}")
_NAME_SANITIZE_RE = re.compile(r"[\u200B\uFEFF]")
_PN_REPAIR_RE = re.compile(r"[+.^:,?\uFFFD]")
_AT_VALUE_RE = re.compile(r"^([^\r\n,]+)")
_DEFAULT_FW_VERSION = "7.5.1.1"
_DEFAULT_AT_VERSION = "1.10"
_WFLKAP_FW_VERSION_THRESHOLD = "8.0.0"
_DEFAULT_WIFI_SCAN_COMMAND_TIMEOUT = 4.0
_DEFAULT_WIFI_SCAN_PREFLIGHT_DELAY = 0.5
_DEFAULT_ANDROID_BLE_TEXT_TIMEOUT = 4.0
_DEFAULT_ESTABLISH_CONNECTION_TIMEOUT = 8.0
_DEFAULT_REFRESH_DEVICE_LOOKUP_TIMEOUT = 2.0
_BLE_READ_QUERY_ATTEMPTS = 4
_VENDOR_NOTIFY_POLL_TIMEOUT = 0.25
_VENDOR_FRAGMENT_SETTLE_TIMEOUT = 0.5
_DEFAULT_RESTART_REQUIRED_FW_VERSIONS = frozenset(
    {
        "7.1.1.1",
        "7.2.1.1",
        "7.3.1.8",
        "7.3.2.1",
        "7.4.1.1",
        "7.5.1.1",
        "7.6.1.1",
        "7.51.2.2",
        "7.52.1.1",
        "7.53.1.1",
        "7.60.3.1",
        "13.1.2.6",
        "13.2.3.6",
    }
)
_LINK_DEGRADED_CODES = frozenset({"W051", "W052", "W301", "W302", "W053"})
_LINK_FAILURE_CODES = frozenset({"W008", "W012", "W049", "W099"})


def normalize_ble_uuid(value: str) -> str:
    """Return one BLE UUID string in canonical lowercase form."""

    normalized = str(value or "").strip().lower()
    if not normalized:
        raise SmartEssBleError("ble_uuid_invalid")
    return normalized


def is_smartess_ble_pn(value: str) -> bool:
    """Return whether one string matches the PN formats used by SmartESS BLE scan."""

    normalized = str(value or "").strip().upper()
    return bool(_PN14_RE.fullmatch(normalized) or _PN18_RE.fullmatch(normalized))


def sanitize_ble_name(value: str) -> str:
    """Trim simple zero-width garbage observed in scanned BLE names."""

    return _NAME_SANITIZE_RE.sub("", str(value or "")).strip()


def _normalize_candidate_pn(value: str) -> str:
    cleaned = sanitize_ble_name(value).upper()
    if is_smartess_ble_pn(cleaned):
        return cleaned
    repaired = _PN_REPAIR_RE.sub("0", cleaned)
    if is_smartess_ble_pn(repaired):
        return repaired
    match = _PN_SEARCH_RE.search(repaired)
    return match.group(0) if match is not None else ""


def _normalize_service_uuids(values: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        normalized.append(normalize_ble_uuid(text))
    return tuple(dict.fromkeys(normalized))


def parse_ble_scan_record(payload: bytes | bytearray) -> SmartEssBleScanRecord:
    """Parse one raw BLE advertisement record the way SmartESS uses it."""

    data = memoryview(bytes(payload))
    position = 0
    flags: int | None = None
    local_name = ""
    local_pn = ""

    while position + 1 < len(data):
        field_len = data[position]
        position += 1
        if field_len == 0:
            break
        if position + field_len > len(data):
            break

        field_type = data[position]
        field_data = bytes(data[position + 1 : position + field_len])
        position += field_len

        if field_type == 0x01 and field_data:
            flags = field_data[0]
            continue
        if field_type in (0x08, 0x09):
            local_name = sanitize_ble_name(field_data.decode("utf-8", errors="ignore"))
            continue
        if field_type == 0xFF:
            pn = _normalize_candidate_pn(field_data.decode("utf-8", errors="ignore"))
            if pn:
                local_pn = pn

    return SmartEssBleScanRecord(local_name=local_name, local_pn=local_pn, flags=flags)


def _extract_pn_from_manufacturer_data(manufacturer_data: object) -> str:
    if not isinstance(manufacturer_data, dict):
        return ""
    for key, value in manufacturer_data.items():
        if value is None:
            continue
        text = bytes(value).decode("utf-8", errors="ignore")
        pn = _normalize_candidate_pn(text)
        if pn:
            return pn
        if isinstance(key, int) and 0 <= key <= 0xFFFF:
            prefix = key.to_bytes(2, "little").decode("ascii", errors="ignore")
            pn = _normalize_candidate_pn(f"{prefix}{text}")
            if pn:
                return pn
    return ""


def build_ble_candidate(
    *,
    address: str,
    device_name: str = "",
    local_name: str = "",
    local_pn: str = "",
    service_uuids: Iterable[str] = (),
    device: object | None = None,
) -> SmartEssBleCandidate | None:
    """Build one normalized SmartESS BLE discovery candidate or return None."""

    normalized_address = str(address or "").strip()
    if not normalized_address:
        raise SmartEssBleError("ble_address_invalid")

    normalized_device_name = sanitize_ble_name(device_name)
    normalized_local_name = sanitize_ble_name(local_name)
    normalized_local_pn = _normalize_candidate_pn(local_pn)
    if is_smartess_ble_pn(normalized_device_name):
        normalized_local_pn = normalized_device_name.upper()
    if not normalized_local_pn:
        return None
    if not normalized_local_name:
        normalized_local_name = normalized_device_name or normalized_local_pn

    return SmartEssBleCandidate(
        address=normalized_address,
        local_pn=normalized_local_pn,
        local_name=normalized_local_name,
        device_name=normalized_device_name,
        service_uuids=_normalize_service_uuids(service_uuids),
        device=device,
    )


def normalize_discovered_candidate(
    *,
    address: str,
    device_name: str = "",
    advertisement_local_name: str = "",
    manufacturer_data: object = None,
    service_uuids: Iterable[str] = (),
    scan_record: bytes | bytearray | None = None,
    device: object | None = None,
) -> SmartEssBleCandidate | None:
    """Normalize one discovered BLE device into a SmartESS candidate."""

    parsed = parse_ble_scan_record(scan_record or b"")
    local_name = parsed.local_name or sanitize_ble_name(advertisement_local_name)
    local_pn = parsed.local_pn or _extract_pn_from_manufacturer_data(manufacturer_data)
    return build_ble_candidate(
        address=address,
        device_name=device_name,
        local_name=local_name,
        local_pn=local_pn,
        service_uuids=service_uuids,
        device=device,
    )


VENDOR_LAYOUT = SmartEssBleUuidLayout(
    name="vendor",
    service_uuid=normalize_ble_uuid("53300000-0023-4BD4-BBD5-A6920E4C5653"),
    write_uuid=normalize_ble_uuid("53300001-0023-4BD4-BBD5-A6920E4C5653"),
    notify_uuid=normalize_ble_uuid("53300005-0023-4BD4-BBD5-A6920E4C5653"),
)

PROVISION_LAYOUT = SmartEssBleUuidLayout(
    name="provision",
    service_uuid=normalize_ble_uuid("00001827-0000-1000-8000-00805F9B34FB"),
    write_uuid=normalize_ble_uuid("00002ADB-0000-1000-8000-00805F9B34FB"),
    notify_uuid=normalize_ble_uuid("00002ADC-0000-1000-8000-00805F9B34FB"),
)

PROXY_LAYOUT = SmartEssBleUuidLayout(
    name="proxy",
    service_uuid=normalize_ble_uuid("00001828-0000-1000-8000-00805F9B34FB"),
    write_uuid=normalize_ble_uuid("00002ADD-0000-1000-8000-00805F9B34FB"),
    notify_uuid=normalize_ble_uuid("00002ADE-0000-1000-8000-00805F9B34FB"),
)


def choose_ble_uuid_layout(service_uuids: Iterable[str]) -> SmartEssBleUuidLayout:
    """Choose the best-known SmartESS UUID layout for discovered services."""

    normalized = {normalize_ble_uuid(value) for value in service_uuids if str(value or "").strip()}
    if PROVISION_LAYOUT.service_uuid in normalized:
        return PROVISION_LAYOUT
    if PROXY_LAYOUT.service_uuid in normalized:
        return PROXY_LAYOUT
    return VENDOR_LAYOUT


def build_ble_text_payload(command: str, *, append_crlf: bool = True) -> bytes:
    """Encode one SmartESS BLE command payload."""

    rendered = str(command or "")
    if not rendered or not rendered.isascii():
        raise SmartEssBleError("ble_command_invalid")
    payload = rendered.encode("utf-8")
    if append_crlf:
        payload += b"\r\n"
    return payload


def decode_ble_text_payload(payload: bytes | bytearray, *, strip_crlf: bool = True) -> str:
    """Decode one SmartESS BLE notify payload into text."""

    data = bytes(payload)
    if strip_crlf:
        data = data.rstrip(b"\r\n")
    return data.decode("utf-8", errors="ignore")


def compare_ble_versions(left: str, right: str) -> int:
    """Compare two dotted numeric version strings."""

    def _parts(value: str) -> list[int]:
        parts: list[int] = []
        for raw_part in str(value or "").split("."):
            raw_part = raw_part.strip()
            if not raw_part:
                parts.append(0)
                continue
            try:
                parts.append(int(raw_part))
            except ValueError:
                parts.append(0)
        return parts

    left_parts = _parts(left)
    right_parts = _parts(right)
    max_len = max(len(left_parts), len(right_parts))
    left_parts.extend([0] * (max_len - len(left_parts)))
    right_parts.extend([0] * (max_len - len(right_parts)))

    if left_parts < right_parts:
        return -1
    if left_parts > right_parts:
        return 1
    return 0


def select_ble_provision_branch(at_version: str) -> SmartEssBleProvisionBranch:
    """Select the SmartESS BLE provisioning branch from the AT interpreter version."""

    normalized = str(at_version or "").strip() or _DEFAULT_AT_VERSION
    if compare_ble_versions(normalized, "1.11") >= 0:
        return SmartEssBleProvisionBranch.WFLKAP
    return SmartEssBleProvisionBranch.INTPARA


def parse_at_response_value(response: str, prefix: str, *, default: str = "") -> str:
    """Extract one simple `AT+...:value` payload, falling back when missing."""

    marker = str(prefix or "")
    text = str(response or "")
    if marker and marker in text:
        tail = text.split(marker, 1)[1].strip()
        match = _AT_VALUE_RE.match(tail)
        if match is not None:
            return match.group(1).strip()
    return default


def parse_wifi_scan_response(response: str) -> tuple[SmartEssBleWifiNetwork, ...]:
    """Parse one collector Wi-Fi scan list response."""

    text = str(response or "").strip()
    tail = ""
    for marker in ("AT+INTPARA:49,", "AT+INTPARA49:", "49,"):
        if marker in text:
            tail = text.split(marker, 1)[1]
            break
    if not tail:
        return ()

    networks: list[SmartEssBleWifiNetwork] = []
    for segment in re.findall(r"\[([^\]]+)\]", tail):
        ssid_raw, separator, signal_raw = segment.rpartition(",")
        if not separator:
            continue
        ssid = ssid_raw.strip()
        signal_text = signal_raw.strip()
        if not ssid:
            continue
        try:
            signal = int(signal_text)
        except ValueError:
            continue
        networks.append(SmartEssBleWifiNetwork(ssid=ssid, signal=signal))

    return tuple(networks)


def _command_response_marker(command: str) -> str:
    text = str(command or "").strip()
    if not text:
        return ""
    if text.startswith("AT+INTPARA") and text.endswith("?"):
        suffix = text.removeprefix("AT+INTPARA")[:-1]
        if suffix.isdigit():
            return f"AT+INTPARA:{suffix}"
    if "=" in text:
        return f"{text.split('=', 1)[0]}:"
    return text.rstrip("?")


def _is_vendor_placeholder_payload(text: str) -> bool:
    stripped = str(text or "").strip()
    return not stripped or "-----" in stripped


def _safe_ble_log_preview(text: str, *, max_len: int = 160) -> str:
    """Return a log preview with known credential readbacks removed."""

    value = str(text or "")
    marker = "AT+INTPARA:43,"
    marker_index = value.find(marker)
    if marker_index >= 0:
        value = f"{value[: marker_index + len(marker)]}<redacted>"
    return value[:max_len]


def _is_wifi_scan_response_payload(text: str) -> bool:
    stripped = str(text or "").strip()
    return any(
        marker in stripped
        for marker in (
            "AT+INTPARA:49",
            "AT+INTPARA49:",
            "49,",
        )
    )


def _is_possible_vendor_response_fragment(text: str, expected_marker: str) -> bool:
    """Return whether *text* may be an incomplete response for *expected_marker*."""

    value = str(text or "").strip()
    marker = str(expected_marker or "").strip()
    if not value or not marker:
        return False
    if marker in value:
        return True
    # Vendor BLE payloads can be split at arbitrary positions. A first fragment
    # such as "AT+WFLKA" must not be treated as a complete response for
    # AT+WFLKAP=...; wait for the next read/notify fragment instead.
    if marker.startswith(value):
        return True
    return value.startswith(marker)


def _can_return_vendor_fragments(text: str, expected_marker: str) -> bool:
    """Return whether buffered vendor fragments are a complete response."""

    marker = str(expected_marker or "").strip()
    if not marker:
        return True
    if marker == "AT+INTPARA:49":
        return True
    return marker in str(text or "")


def extract_at_status_code(response: str, prefix: str) -> str:
    """Extract the `W...` status code from one AT response."""

    value = parse_at_response_value(response, f"{prefix}:")
    return value if value.startswith("W") else ""


def parse_link_provision_result(response: str) -> tuple[SmartEssBleProvisionOutcome, str]:
    """Normalize one `AT+LINK?` reply into an outcome and status code."""

    code = extract_at_status_code(response, "AT+LINK") or "Timeout"
    if code == "W000":
        return SmartEssBleProvisionOutcome.SUCCESS, code
    if code in _LINK_DEGRADED_CODES:
        return SmartEssBleProvisionOutcome.DEGRADED, code
    if code in _LINK_FAILURE_CODES:
        return SmartEssBleProvisionOutcome.FAILURE, code
    return SmartEssBleProvisionOutcome.FAILURE, code


def parse_intpara48_response(response: str) -> tuple[str, str, str] | None:
    """Parse the three result fields from `AT+INTPARA:48,...`."""

    marker = "AT+INTPARA:48,"
    text = str(response or "")
    if marker not in text:
        return None

    tail = text.split(marker, 1)[1].strip()
    parts = [part.strip() for part in tail.split(",", 2)]
    if len(parts) != 3:
        return None
    return tuple(part.split()[0] if part else "" for part in parts)  # type: ignore[return-value]


def parse_intpara48_provision_result(
    response: str,
) -> tuple[SmartEssBleProvisionOutcome, str, tuple[str, str, str] | None]:
    """Normalize one `AT+INTPARA48?` reply using SmartESS branch-two rules."""

    details = parse_intpara48_response(response)
    if details is None:
        return SmartEssBleProvisionOutcome.FAILURE, "Timeout", None

    detected, station, cloud = details
    combined = f"{station}{cloud}"
    if station not in {"0", "1"} or cloud not in {"0", "1"} or combined == "11":
        return SmartEssBleProvisionOutcome.FAILURE, "W008", details
    if combined == "10":
        return SmartEssBleProvisionOutcome.DEGRADED, "W008", details
    if combined == "01":
        return SmartEssBleProvisionOutcome.DEGRADED, "W051", details
    try:
        detected_count = int(detected)
    except ValueError:
        detected_count = 0
    if detected_count > 0:
        return SmartEssBleProvisionOutcome.SUCCESS, "W000", details
    return SmartEssBleProvisionOutcome.DEGRADED, "W302", details


def _capability_reason_from_exception(exc: BaseException) -> str:
    detail = str(exc).lower()
    if "adapter" in detail and ("not found" in detail or "no bluetooth" in detail or "not available" in detail):
        return "adapter_not_found"
    if "permission" in detail or "access denied" in detail or "not authorized" in detail:
        return "permission_denied"
    if isinstance(exc, PermissionError):
        return "permission_denied"
    if isinstance(exc, FileNotFoundError):
        return "adapter_not_found"
    if isinstance(exc, NotImplementedError):
        return "backend_not_supported"
    if isinstance(exc, OSError):
        return "host_unavailable"
    return "probe_failed"


async def async_probe_ble_host_capability(
    *,
    probe: Callable[[], Awaitable[object]] | None = None,
) -> SmartEssBleHostCapability:
    """Return whether the local host can support the planned BLE bootstrap path."""

    if probe is None:
        if importlib.util.find_spec("bleak") is None:
            return SmartEssBleHostCapability(
                available=False,
                backend="bleak",
                reason="backend_missing",
                detail="bleak is not importable on this host",
            )
        try:
            from bleak import BleakScanner  # type: ignore[import-not-found]

            try:
                await BleakScanner.discover(timeout=0.1, return_adv=True)
            except TypeError:
                await BleakScanner.discover(timeout=0.1)
        except BaseException as exc:
            return SmartEssBleHostCapability(
                available=False,
                backend="bleak",
                reason=_capability_reason_from_exception(exc),
                detail=str(exc),
            )
        return SmartEssBleHostCapability(
            available=True,
            backend="bleak",
            reason="backend_available",
        )

    try:
        await probe()
    except BaseException as exc:
        return SmartEssBleHostCapability(
            available=False,
            backend="probe",
            reason=_capability_reason_from_exception(exc),
            detail=str(exc),
        )

    return SmartEssBleHostCapability(
        available=True,
        backend="probe",
        reason="probe_succeeded",
    )


class SmartEssBleLink(Protocol):
    """Small async BLE link contract used by the SmartESS provisioning session."""

    async def connect(self) -> Sequence[str]:
        """Connect and return the discovered service UUIDs."""

    async def disconnect(self) -> None:
        """Disconnect the BLE link."""

    async def start_notify(self, characteristic_uuid: str, callback: Callable[[bytes], None]) -> None:
        """Register one notify callback for the given characteristic."""

    async def stop_notify(self, characteristic_uuid: str) -> None:
        """Stop notify delivery for the given characteristic."""

    async def write(self, characteristic_uuid: str, data: bytes, *, response: bool = False) -> None:
        """Write raw bytes to one characteristic."""

    async def read(self, characteristic_uuid: str) -> bytes:
        """Read raw bytes from one characteristic."""


class _BleakServiceLike(Protocol):
    uuid: str


class _BleakClientLike(Protocol):
    services: object

    async def connect(self) -> None:
        """Connect the client."""

    async def disconnect(self) -> None:
        """Disconnect the client."""

    async def start_notify(self, characteristic_uuid: str, callback: Callable[[Any, bytes], None]) -> None:
        """Register one notify callback."""

    async def stop_notify(self, characteristic_uuid: str) -> None:
        """Stop notify delivery."""

    async def write_gatt_char(self, characteristic_uuid: str, data: bytes, response: bool = False) -> None:
        """Write one GATT characteristic."""

    async def read_gatt_char(self, characteristic_uuid: str) -> bytes:
        """Read one GATT characteristic."""


class _BleakDeviceLike(Protocol):
    address: str
    name: str | None
    metadata: object


class _BleakAdvertisementLike(Protocol):
    local_name: str | None
    service_uuids: Sequence[str] | None
    manufacturer_data: object


def _default_bleak_client_factory(address: str, *, device: object | None = None) -> _BleakClientLike:
    if importlib.util.find_spec("bleak") is None:
        raise SmartEssBleError("ble_backend_missing")
    from bleak import BleakClient  # type: ignore[import-not-found]

    return BleakClient(device if device is not None else address)


async def _default_bleak_connect_client(
    address: str,
    *,
    device: object | None = None,
) -> _BleakClientLike:
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    phase = "backend_check"
    try:
        if importlib.util.find_spec("bleak") is None:
            raise SmartEssBleError("ble_backend_missing")

        from bleak import BleakClient, BleakScanner  # type: ignore[import-not-found]

        retry_connector_available = importlib.util.find_spec("bleak_retry_connector") is not None
        logger.info(
            "SmartESS BLE default connect start address=%s retry_connector=%s device=%s",
            address,
            retry_connector_available,
            _ble_device_log_summary(device),
        )

        async def _find_fresh_device_by_address(
            *,
            timeout: float,
        ) -> object | None:
            find_device_by_address = getattr(BleakScanner, "find_device_by_address", None)
            if not callable(find_device_by_address):
                return None
            try:
                logger.info(
                    "SmartESS BLE default connect phase=%s start address=%s",
                    "refresh_device_by_address",
                    address,
                )
                try:
                    fresh_device = await find_device_by_address(address, timeout=timeout)
                except TypeError:
                    fresh_device = await find_device_by_address(address)
                logger.info(
                    "SmartESS BLE default connect phase=%s complete address=%s refreshed_device=%s elapsed=%.2fs",
                    "refresh_device_by_address",
                    address,
                    _ble_device_log_summary(fresh_device),
                    loop.time() - started_at,
                )
                return fresh_device
            except Exception as exc:
                logger.warning(
                    "SmartESS BLE default connect phase=%s failed address=%s elapsed=%.2fs error_type=%s error=%s",
                    "refresh_device_by_address",
                    address,
                    loop.time() - started_at,
                    type(exc).__name__,
                    exc,
                )
                return None

        async def _connect_address_only_client() -> _BleakClientLike:
            fresh_device = await _find_fresh_device_by_address(timeout=8.0)
            client_target = fresh_device if fresh_device is not None else address
            client = BleakClient(client_target)
            logger.info(
                "SmartESS BLE default connect phase=%s address=%s client_type=%s target=%s",
                "address_only_bleak_client_connect",
                address,
                type(client).__name__,
                _ble_device_log_summary(fresh_device) if fresh_device is not None else address,
            )
            await client.connect()
            logger.info(
                "SmartESS BLE default connect complete address=%s phase=%s elapsed=%.2fs client_type=%s",
                address,
                "address_only_bleak_client_connect",
                loop.time() - started_at,
                type(client).__name__,
            )
            return client

        if not retry_connector_available:
            phase = "bleak_client_connect"
            client = BleakClient(device if device is not None else address)
            logger.info(
                "SmartESS BLE default connect phase=%s address=%s client_type=%s",
                phase,
                address,
                type(client).__name__,
            )
            await client.connect()
            logger.info(
                "SmartESS BLE default connect complete address=%s phase=%s elapsed=%.2fs client_type=%s",
                address,
                phase,
                loop.time() - started_at,
                type(client).__name__,
            )
            return client

        from bleak_retry_connector import (  # type: ignore[import-not-found]
            BleakClientWithServiceCache,
            establish_connection,
        )

        resolved_device = device
        if resolved_device is None:
            phase = "find_device_by_address"
            find_device_by_address = getattr(BleakScanner, "find_device_by_address", None)
            if callable(find_device_by_address):
                logger.info(
                    "SmartESS BLE default connect phase=%s start address=%s",
                    phase,
                    address,
                )
                try:
                    resolved_device = await find_device_by_address(address, timeout=8.0)
                except TypeError:
                    resolved_device = await find_device_by_address(address)
                logger.info(
                    "SmartESS BLE default connect phase=%s complete address=%s resolved_device=%s elapsed=%.2fs",
                    phase,
                    address,
                    _ble_device_log_summary(resolved_device),
                    loop.time() - started_at,
                )

        if resolved_device is not None:
            refreshed_device = await _find_fresh_device_by_address(
                timeout=_DEFAULT_REFRESH_DEVICE_LOOKUP_TIMEOUT,
            )
            if refreshed_device is not None:
                resolved_device = refreshed_device

        if resolved_device is None:
            phase = "address_only_bleak_client_connect"
            return await _connect_address_only_client()

        phase = "establish_connection"
        logger.info(
            "SmartESS BLE default connect phase=%s start address=%s resolved_device=%s",
            phase,
            address,
            _ble_device_log_summary(resolved_device),
        )
        try:
            client = await asyncio.wait_for(
                establish_connection(
                    BleakClientWithServiceCache,
                    resolved_device,
                    name=str(getattr(resolved_device, "name", None) or address),
                    max_attempts=3,
                ),
                timeout=_DEFAULT_ESTABLISH_CONNECTION_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "SmartESS BLE default connect establish_connection failed address=%s elapsed=%.2fs error_type=%s error=%s; "
                "falling back to address-only connect",
                address,
                loop.time() - started_at,
                type(exc).__name__,
                exc,
            )
            phase = "address_only_bleak_client_connect"
            return await _connect_address_only_client()
        logger.info(
            "SmartESS BLE default connect complete address=%s phase=%s elapsed=%.2fs client_type=%s",
            address,
            phase,
            loop.time() - started_at,
            type(client).__name__,
        )
        return client
    except BaseException as exc:
        logger.warning(
            "SmartESS BLE default connect interrupted address=%s phase=%s elapsed=%.2fs "
            "error_type=%s error=%s device=%s",
            address,
            phase,
            loop.time() - started_at,
            type(exc).__name__,
            exc,
            _ble_device_log_summary(device),
        )
        raise


async def _default_bleak_discover(timeout: float) -> object:
    if importlib.util.find_spec("bleak") is None:
        raise SmartEssBleError("ble_backend_missing")
    from bleak import BleakScanner  # type: ignore[import-not-found]

    try:
        return await BleakScanner.discover(timeout=timeout, return_adv=True)
    except TypeError:
        return await BleakScanner.discover(timeout=timeout)


def _extract_service_uuids(services: object) -> tuple[str, ...]:
    items: Iterable[object]
    mapping = getattr(services, "services", None)
    if isinstance(mapping, dict):
        items = mapping.values()
    elif mapping is not None:
        items = mapping
    else:
        items = services if isinstance(services, Iterable) else ()

    normalized: list[str] = []
    for item in items:
        uuid = getattr(item, "uuid", "")
        if not uuid:
            continue
        normalized.append(normalize_ble_uuid(uuid))
    return tuple(dict.fromkeys(normalized))


def _iter_discovered_records(discovered: object) -> Iterable[tuple[object, object | None]]:
    if isinstance(discovered, dict):
        for value in discovered.values():
            if isinstance(value, tuple) and len(value) == 2:
                yield value[0], value[1]
            else:
                yield value, None
        return

    if isinstance(discovered, Iterable):
        for value in discovered:
            if isinstance(value, tuple) and len(value) == 2:
                yield value[0], value[1]
            else:
                yield value, None


def _metadata_mapping(device: object) -> dict[str, object]:
    metadata = getattr(device, "metadata", None)
    return metadata if isinstance(metadata, dict) else {}


def _safe_log_value(value: object, *, limit: int = 140) -> str:
    try:
        text = str(value)
    except Exception:
        text = f"<{type(value).__name__}>"
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _ble_device_log_summary(device: object | None) -> str:
    if device is None:
        return "none"

    parts = [f"type={type(device).__name__}"]
    for attribute in ("address", "name", "rssi"):
        value = getattr(device, attribute, None)
        if value not in (None, ""):
            parts.append(f"{attribute}={_safe_log_value(value)}")

    details = getattr(device, "details", None)
    if details is not None:
        parts.append(f"details_type={type(details).__name__}")
        if isinstance(details, dict):
            keys = ",".join(sorted(str(key) for key in details)[:8])
            if keys:
                parts.append(f"details_keys={keys}")

    metadata = getattr(device, "metadata", None)
    if isinstance(metadata, dict):
        keys = ",".join(sorted(str(key) for key in metadata)[:8])
        if keys:
            parts.append(f"metadata_keys={keys}")
        service_uuids = metadata.get("uuids") or metadata.get("service_uuids")
        if service_uuids:
            rendered = ",".join(str(value) for value in list(service_uuids)[:6])
            parts.append(f"metadata_uuids={_safe_log_value(rendered)}")
        manufacturer_data = metadata.get("manufacturer_data")
        if isinstance(manufacturer_data, dict):
            ids = ",".join(str(key) for key in sorted(manufacturer_data)[:8])
            if ids:
                parts.append(f"manufacturer_ids={ids}")

    return " ".join(parts)


class BleakSmartEssBleScanner:
    """Thin adapter that discovers and normalizes SmartESS BLE candidates."""

    def __init__(
        self,
        *,
        discover: Callable[[float], Awaitable[object]] | None = None,
    ) -> None:
        self._discover = discover or _default_bleak_discover

    async def discover_candidates(self, *, timeout: float = 5.0) -> tuple[SmartEssBleCandidate, ...]:
        discovered = await self._discover(float(timeout))
        deduped: dict[str, SmartEssBleCandidate] = {}

        for device, advertisement in _iter_discovered_records(discovered):
            metadata = _metadata_mapping(device)
            service_uuids = getattr(advertisement, "service_uuids", None) or metadata.get("uuids") or ()
            candidate = normalize_discovered_candidate(
                address=str(getattr(device, "address", "") or "").strip(),
                device_name=str(getattr(device, "name", "") or "").strip(),
                advertisement_local_name=str(getattr(advertisement, "local_name", "") or "").strip(),
                manufacturer_data=(
                    getattr(advertisement, "manufacturer_data", None)
                    if advertisement is not None
                    else metadata.get("manufacturer_data")
                ),
                service_uuids=service_uuids,
                device=device,
            )
            if candidate is not None:
                deduped[candidate.address] = candidate

        return tuple(deduped.values())


class BleakSmartEssBleLink:
    """Thin adapter from the session link protocol to one Bleak-style client."""

    def __init__(
        self,
        address: str,
        *,
        client_factory: Callable[[str], _BleakClientLike] | None = None,
        connect_client: Callable[[str], Awaitable[_BleakClientLike]] | None = None,
        device: object | None = None,
    ) -> None:
        if not str(address or "").strip():
            raise SmartEssBleError("ble_address_invalid")
        self._address = str(address).strip()
        self._device = device
        self._client_factory = client_factory or (
            lambda resolved_address: _default_bleak_client_factory(
                resolved_address,
                device=self._device,
            )
        )
        self._connect_client = connect_client if connect_client is not None else (
            (
                lambda resolved_address: _default_bleak_connect_client(
                    resolved_address,
                    device=self._device,
                )
            )
            if client_factory is None
            else None
        )
        self._client: _BleakClientLike | None = None

    async def connect(self) -> Sequence[str]:
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        phase = "reuse_client"
        try:
            logger.info(
                "SmartESS BLE link connect start address=%s device=%s connect_client=%s client_cached=%s",
                self._address,
                _ble_device_log_summary(self._device),
                self._connect_client is not None,
                self._client is not None,
            )
            if self._client is None:
                if self._connect_client is not None:
                    phase = "connect_client"
                    logger.info(
                        "SmartESS BLE link connect phase=%s start address=%s",
                        phase,
                        self._address,
                    )
                    self._client = await self._connect_client(self._address)
                else:
                    phase = "client_factory_connect"
                    logger.info(
                        "SmartESS BLE link connect phase=%s start address=%s",
                        phase,
                        self._address,
                    )
                    self._client = self._client_factory(self._address)
                    await self._client.connect()
                logger.info(
                    "SmartESS BLE link connect phase=%s complete address=%s elapsed=%.2fs client_type=%s",
                    phase,
                    self._address,
                    loop.time() - started_at,
                    type(self._client).__name__,
                )

            services_method = getattr(self._client, "get_services", None)
            phase = "get_services" if callable(services_method) else "client_services"
            logger.info(
                "SmartESS BLE link connect phase=%s start address=%s client_type=%s",
                phase,
                self._address,
                type(self._client).__name__,
            )
            if callable(services_method):
                services = await services_method()
            else:
                services = getattr(self._client, "services", None)
            service_uuids = _extract_service_uuids(services)
            logger.info(
                "SmartESS BLE link connect complete address=%s phase=%s elapsed=%.2fs service_uuids=%s",
                self._address,
                phase,
                loop.time() - started_at,
                service_uuids,
            )
            return service_uuids
        except BaseException as exc:
            logger.warning(
                "SmartESS BLE link connect interrupted address=%s phase=%s elapsed=%.2fs "
                "error_type=%s error=%s device=%s",
                self._address,
                phase,
                loop.time() - started_at,
                type(exc).__name__,
                exc,
                _ble_device_log_summary(self._device),
            )
            raise

    async def disconnect(self) -> None:
        if self._client is None:
            return
        await self._client.disconnect()

    async def start_notify(self, characteristic_uuid: str, callback: Callable[[bytes], None]) -> None:
        if self._client is None:
            raise SmartEssBleError("ble_not_connected")

        def _callback(_sender: Any, data: bytes) -> None:
            callback(bytes(data))

        await self._client.start_notify(characteristic_uuid, _callback)

    async def stop_notify(self, characteristic_uuid: str) -> None:
        if self._client is None:
            return
        await self._client.stop_notify(characteristic_uuid)

    async def write(self, characteristic_uuid: str, data: bytes, *, response: bool = False) -> None:
        if self._client is None:
            raise SmartEssBleError("ble_not_connected")
        await self._client.write_gatt_char(characteristic_uuid, data, response=response)

    async def read(self, characteristic_uuid: str) -> bytes:
        if self._client is None:
            raise SmartEssBleError("ble_not_connected")
        return bytes(await self._client.read_gatt_char(characteristic_uuid))


class SmartEssBleSession:
    """Raw SmartESS BLE session with UUID negotiation and notify buffering."""

    def __init__(self, link: SmartEssBleLink) -> None:
        self._link = link
        self._layout: SmartEssBleUuidLayout | None = None
        self._connected = False
        self._notifications: asyncio.Queue[bytes] = asyncio.Queue()

    @property
    def connected(self) -> bool:
        """Return whether the session has an active BLE link."""

        return self._connected

    @property
    def layout(self) -> SmartEssBleUuidLayout:
        """Return the currently selected BLE UUID layout."""

        if self._layout is None:
            raise SmartEssBleError("ble_layout_not_selected")
        return self._layout

    async def connect(self) -> SmartEssBleUuidLayout:
        """Connect the BLE link, negotiate UUIDs, and subscribe to notify."""

        if self._connected and self._layout is not None:
            return self._layout

        service_uuids = await self._link.connect()
        self._layout = choose_ble_uuid_layout(service_uuids)
        logger.info(
            "SmartESS BLE session connected service_uuids=%s selected_layout=%s",
            tuple(service_uuids),
            self._layout.name,
        )
        self._drain_notifications()
        await self._link.start_notify(self._layout.notify_uuid, self._handle_notification)
        self._connected = True
        return self._layout

    async def disconnect(self) -> None:
        """Disconnect the BLE link and clear any buffered notifications."""

        if self._connected and self._layout is not None:
            await self._link.stop_notify(self._layout.notify_uuid)
        await self._link.disconnect()
        self._connected = False
        self._layout = None
        self._drain_notifications()

    async def send_bytes(self, payload: bytes, *, response: bool = False) -> None:
        """Write one raw payload using the negotiated write characteristic."""

        if not self._connected or self._layout is None:
            raise SmartEssBleError("ble_not_connected")
        await self._link.write(self._layout.write_uuid, bytes(payload), response=response)

    async def wait_for_notification(self, *, timeout: float = 3.0) -> bytes:
        """Wait for one notify payload from the collector."""

        try:
            return await asyncio.wait_for(self._notifications.get(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            logger.warning(
                "SmartESS BLE notification wait timed out layout=%s timeout=%.1fs",
                self._layout.name if self._layout is not None else "unknown",
                timeout,
            )
            raise SmartEssBleError("ble_notification_timeout") from exc

    async def read_bytes(self, *, timeout: float = 3.0) -> bytes:
        """Read one raw payload from the currently selected BLE characteristic."""

        if not self._connected or self._layout is None:
            raise SmartEssBleError("ble_not_connected")
        return await asyncio.wait_for(
            self._link.read(self._layout.write_uuid),
            timeout=timeout,
        )

    async def exchange_text(
        self,
        command: str,
        *,
        timeout: float = 3.0,
        append_crlf: bool = True,
        response: bool = False,
        drain_before_send: bool = True,
    ) -> str:
        """Send one text command and decode the next notify payload as text."""

        if drain_before_send:
            self._drain_notifications()
        await self.send_bytes(
            build_ble_text_payload(command, append_crlf=append_crlf),
            response=response,
        )
        if self._layout is not None and self._layout.name == VENDOR_LAYOUT.name:
            return await self._exchange_vendor_text(command, timeout=timeout)
        payload = await self.wait_for_notification(timeout=timeout)
        return decode_ble_text_payload(payload)

    async def _exchange_vendor_text(self, command: str, *, timeout: float) -> str:
        if not self._connected or self._layout is None:
            raise SmartEssBleError("ble_not_connected")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(timeout))
        expected_marker = _command_response_marker(command)
        fragments: list[str] = []
        last_fragment_at: float | None = None
        read_attempts = 0
        read_errors = 0
        last_source = "none"

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                if fragments:
                    combined = "".join(fragments)
                    if _can_return_vendor_fragments(combined, expected_marker):
                        return combined
                logger.debug(
                    "SmartESS BLE vendor response timed out command=%s marker=%s read_attempts=%d "
                    "read_errors=%d last_source=%s",
                    command,
                    expected_marker or "none",
                    read_attempts,
                    read_errors,
                    last_source,
                )
                raise SmartEssBleError("ble_notification_timeout")

            text, source = await self._poll_vendor_text_response(
                timeout=remaining,
                prefer_read=bool(fragments),
            )
            last_source = source
            if source in {"read", "read_empty", "read_error"}:
                read_attempts += 1
            if source == "read_error":
                read_errors += 1
            if text is None:
                if (
                    fragments
                    and last_fragment_at is not None
                    and loop.time() - last_fragment_at >= _VENDOR_FRAGMENT_SETTLE_TIMEOUT
                ):
                    combined = "".join(fragments)
                    if _can_return_vendor_fragments(combined, expected_marker):
                        return combined
                continue

            if command == "AT+ATVER?" and not text:
                return text

            if _is_vendor_placeholder_payload(text):
                if fragments:
                    return "".join(fragments)
                continue

            if expected_marker == "AT+INTPARA:49":
                if _is_wifi_scan_response_payload(text):
                    fragments.append(text)
                    last_fragment_at = loop.time()
                    continue
                if fragments:
                    if "[" in text and "]" in text:
                        fragments.append(text)
                        last_fragment_at = loop.time()
                        continue
                    return "".join(fragments)
                logger.debug(
                    "SmartESS BLE ignoring non-Wi-Fi vendor response while waiting for scan payload preview=%s",
                    _safe_ble_log_preview(text),
                )
                continue

            if expected_marker and expected_marker in text:
                return text
            if expected_marker:
                combined = "".join(fragments) + text if fragments else text
                if expected_marker in combined:
                    return combined
                if _is_possible_vendor_response_fragment(combined, expected_marker):
                    fragments.append(text)
                    last_fragment_at = loop.time()
                    continue
                logger.debug(
                    "SmartESS BLE ignoring unrelated vendor response while waiting for marker=%s preview=%s",
                    expected_marker,
                    _safe_ble_log_preview(text),
                )
                continue
            if text:
                return text

    async def _poll_vendor_text_response(
        self,
        *,
        timeout: float,
        prefer_read: bool = False,
    ) -> tuple[str | None, str]:
        notify_timeout = 0.0 if prefer_read else min(_VENDOR_NOTIFY_POLL_TIMEOUT, timeout)
        if notify_timeout > 0:
            try:
                payload = await asyncio.wait_for(self._notifications.get(), timeout=notify_timeout)
            except asyncio.TimeoutError:
                payload = None
            if payload is not None:
                text = decode_ble_text_payload(payload)
                logger.debug(
                    "SmartESS BLE vendor response via notify layout=%s preview=%s",
                    self._layout.name if self._layout is not None else "unknown",
                    _safe_ble_log_preview(text),
                )
                return text, "notify"

        read_timeout = max(0.0, min(timeout, _VENDOR_NOTIFY_POLL_TIMEOUT))
        if read_timeout <= 0 or self._layout is None:
            return None, "none"
        try:
            payload = await self.read_bytes(timeout=read_timeout)
        except asyncio.TimeoutError:
            return None, "none"
        except Exception as exc:
            logger.debug(
                "SmartESS BLE vendor read failed layout=%s error=%s",
                self._layout.name,
                exc,
            )
            return None, "read_error"
        text = decode_ble_text_payload(payload)
        if text:
            logger.debug(
                "SmartESS BLE vendor response via read layout=%s preview=%s",
                self._layout.name,
                _safe_ble_log_preview(text),
            )
            return text, "read"
        return None, "read_empty"

    def _handle_notification(self, payload: bytes) -> None:
        self._notifications.put_nowait(bytes(payload))

    def _drain_notifications(self) -> None:
        while not self._notifications.empty():
            self._notifications.get_nowait()


class _SmartEssBleTextSessionLike(Protocol):
    async def exchange_text(
        self,
        command: str,
        *,
        timeout: float = 3.0,
        append_crlf: bool = True,
        response: bool = False,
        drain_before_send: bool = True,
    ) -> str:
        """Send one text command and return the next text response."""


class SmartEssBleProvisioner:
    """Collector-specific SmartESS BLE provisioning workflow."""

    def __init__(
        self,
        session: _SmartEssBleTextSessionLike,
        *,
        command_timeout: float = 3.0,
        status_timeout: float = 3.0,
        status_poll_interval: float = 1.0,
        max_status_polls: int = 5,
        restart_required_fw_versions: Iterable[str] = _DEFAULT_RESTART_REQUIRED_FW_VERSIONS,
        wifi_scan_preflight_delay: float = _DEFAULT_WIFI_SCAN_PREFLIGHT_DELAY,
    ) -> None:
        self._session = session
        self._command_timeout = float(command_timeout)
        self._status_timeout = float(status_timeout)
        self._status_poll_interval = max(0.0, float(status_poll_interval))
        self._max_status_polls = max(1, int(max_status_polls))
        self._restart_required_fw_versions = frozenset(str(value).strip() for value in restart_required_fw_versions)
        self._wifi_scan_preflight_delay = max(0.0, float(wifi_scan_preflight_delay))
        self._wifi_scan_preflight_done = False
        self._last_firmware_version = ""

    @property
    def last_firmware_version(self) -> str:
        """Return the last collector firmware version read during this session."""

        return self._last_firmware_version

    async def read_firmware_version(self) -> str:
        """Read `AT+FWVER?`, matching the SmartESS default fallback."""

        try:
            response = await self._exchange_read_query(
                "AT+FWVER?",
                timeout=max(self._command_timeout, _DEFAULT_ANDROID_BLE_TEXT_TIMEOUT),
            )
        finally:
            self._wifi_scan_preflight_done = True
        firmware_version = parse_at_response_value(response, "AT+FWVER:")
        if firmware_version:
            self._last_firmware_version = firmware_version
            return firmware_version
        return _DEFAULT_FW_VERSION

    async def read_at_version(self) -> str:
        """Read `AT+ATVER?`, matching the SmartESS default fallback."""

        response = await self._exchange_read_query(
            "AT+ATVER?",
            timeout=max(self._command_timeout, _DEFAULT_ANDROID_BLE_TEXT_TIMEOUT),
        )
        return parse_at_response_value(response, "AT+ATVER:")

    async def _exchange_read_query(self, command: str, *, timeout: float) -> str:
        """Run one idempotent BLE AT query with the retry policy used by SmartESS.

        Field collectors can acknowledge the GATT write while ignoring the first
        AT query.  The Android client retries read-only commands up to four times;
        mirror that behavior here without ever applying it to provisioning writes.
        """

        if not str(command or "").strip().endswith("?"):
            raise SmartEssBleError("ble_query_command_required")

        last_error: SmartEssBleError | None = None
        for attempt in range(_BLE_READ_QUERY_ATTEMPTS):
            logger.debug(
                "SmartESS BLE read query command=%s attempt=%d/%d timeout=%.1fs",
                command,
                attempt + 1,
                _BLE_READ_QUERY_ATTEMPTS,
                timeout,
            )
            try:
                return await self._session.exchange_text(
                    command,
                    timeout=timeout,
                    append_crlf=False,
                    response=True,
                )
            except SmartEssBleError as exc:
                last_error = exc
                if (
                    str(exc) != "ble_notification_timeout"
                    or attempt == _BLE_READ_QUERY_ATTEMPTS - 1
                ):
                    raise
        assert last_error is not None
        raise last_error

    async def query_device_info(
        self,
        *,
        known_fw_version: str = "",
    ) -> SmartEssBleProvisioningInfo:
        """Read the collector versions and derive the provisioning branch."""

        known_fw_version = str(known_fw_version or "").strip()
        fw_version = str(known_fw_version or self._last_firmware_version or "").strip()
        fw_version_known = bool(fw_version)
        at_version = _DEFAULT_AT_VERSION
        at_version_known = False
        if not fw_version_known:
            try:
                fw_version = await self.read_firmware_version()
                fw_version_known = bool(self._last_firmware_version)
            except SmartEssBleError as exc:
                if str(exc) != "ble_notification_timeout":
                    raise
                fw_version = _DEFAULT_FW_VERSION
                logger.warning(
                    "SmartESS BLE firmware version probe timed out; using fallback version=%s",
                    _DEFAULT_FW_VERSION,
                )
        if fw_version_known:
            self._last_firmware_version = fw_version
        try:
            probed_at_version = await self.read_at_version()
            if probed_at_version:
                at_version = probed_at_version
                at_version_known = True
        except SmartEssBleError as exc:
            if str(exc) != "ble_notification_timeout":
                raise
            logger.warning(
                "SmartESS BLE AT version probe timed out; using fallback version=%s",
                _DEFAULT_AT_VERSION,
            )
        branch = select_ble_provision_branch(at_version)
        if (
            branch == SmartEssBleProvisionBranch.INTPARA
            and fw_version_known
            and compare_ble_versions(fw_version, _WFLKAP_FW_VERSION_THRESHOLD) >= 0
        ):
            logger.warning(
                "SmartESS BLE AT version probe was unavailable or ambiguous for fw_version=%s; "
                "preferring WFLKAP provisioning",
                fw_version,
            )
            branch = SmartEssBleProvisionBranch.WFLKAP
        elif branch == SmartEssBleProvisionBranch.INTPARA and not at_version_known and not fw_version_known:
            logger.warning(
                "SmartESS BLE version probes were unavailable; preferring WFLKAP provisioning instead of legacy INTPARA fallback"
            )
            branch = SmartEssBleProvisionBranch.WFLKAP
        requires_restart = branch == SmartEssBleProvisionBranch.INTPARA and fw_version in self._restart_required_fw_versions
        return SmartEssBleProvisioningInfo(
            fw_version=fw_version,
            at_version=at_version,
            branch=branch,
            requires_restart=requires_restart,
        )

    async def scan_wifi_networks(self) -> tuple[SmartEssBleWifiNetwork, ...]:
        """Read one collector-side Wi-Fi scan list via `AT+INTPARA49?`."""

        await self._prepare_wifi_scan()
        # The Android clients use a four-second window and retry the idempotent
        # scan query up to four times.  Some collectors consistently answer only
        # the second request even with a strong BLE signal.
        per_attempt_timeout = max(self._command_timeout, _DEFAULT_WIFI_SCAN_COMMAND_TIMEOUT)
        response = await self._exchange_read_query(
            "AT+INTPARA49?",
            timeout=per_attempt_timeout,
        )
        return parse_wifi_scan_response(response)

    async def _prepare_wifi_scan(self) -> None:
        if self._wifi_scan_preflight_done:
            return
        logger.debug("SmartESS BLE Wi-Fi scan preflight command=AT+FWVER?")
        try:
            await self.read_firmware_version()
        except SmartEssBleError as exc:
            if str(exc) != "ble_notification_timeout":
                raise
            logger.debug(
                "SmartESS BLE Wi-Fi scan preflight timed out; continuing with scan command"
            )
        if self._wifi_scan_preflight_delay > 0:
            await asyncio.sleep(self._wifi_scan_preflight_delay)

    async def provision_wifi(
        self,
        *,
        ssid: str,
        password: str,
        info: SmartEssBleProvisioningInfo | None = None,
    ) -> SmartEssBleProvisionResult:
        """Provision one collector onto Wi-Fi using the SmartESS BLE AT flow."""

        self._validate_wifi_value(ssid, error_code="ble_wifi_ssid_invalid")
        self._validate_wifi_value(password, error_code="ble_wifi_password_invalid")
        resolved_info = info or await self.query_device_info()
        if resolved_info.branch == SmartEssBleProvisionBranch.WFLKAP:
            return await self._provision_with_wflkap(ssid=ssid, password=password)
        return await self._provision_with_intpara(
            ssid=ssid,
            password=password,
            requires_restart=resolved_info.requires_restart,
        )

    async def _provision_with_wflkap(self, *, ssid: str, password: str) -> SmartEssBleProvisionResult:
        command_timeout = max(self._command_timeout, _DEFAULT_ANDROID_BLE_TEXT_TIMEOUT)
        try:
            response = await self._session.exchange_text(
                f"AT+WFLKAP={ssid},AES,WPA2_PSK,{password}",
                timeout=command_timeout,
                append_crlf=False,
                response=True,
            )
        except SmartEssBleError as exc:
            if str(exc) != "ble_notification_timeout":
                raise
            response = ""
        status_code = extract_at_status_code(response, "AT+WFLKAP") or "Timeout"
        if response and status_code != "W000":
            return SmartEssBleProvisionResult(
                branch=SmartEssBleProvisionBranch.WFLKAP,
                outcome=SmartEssBleProvisionOutcome.FAILURE,
                status_code=status_code,
                raw_response=response,
            )
        return await self._poll_link_result()

    async def _provision_with_intpara(
        self,
        *,
        ssid: str,
        password: str,
        requires_restart: bool,
    ) -> SmartEssBleProvisionResult:
        command_timeout = max(self._command_timeout, _DEFAULT_ANDROID_BLE_TEXT_TIMEOUT)
        try:
            response = await self._session.exchange_text(
                f"AT+INTPARA=41,{ssid}",
                timeout=command_timeout,
                append_crlf=False,
                response=True,
            )
        except SmartEssBleError as exc:
            if str(exc) != "ble_notification_timeout":
                raise
            response = ""
        status_code = extract_at_status_code(response, "AT+INTPARA") or "Timeout"
        if response and status_code != "W000":
            return SmartEssBleProvisionResult(
                branch=SmartEssBleProvisionBranch.INTPARA,
                outcome=SmartEssBleProvisionOutcome.FAILURE,
                status_code=status_code,
                raw_response=response,
            )

        try:
            response = await self._session.exchange_text(
                f"AT+INTPARA=43,{password}",
                timeout=command_timeout,
                response=True,
            )
        except SmartEssBleError as exc:
            if str(exc) != "ble_notification_timeout":
                raise
            response = ""
        status_code = extract_at_status_code(response, "AT+INTPARA") or "Timeout"
        if response and status_code != "W000":
            return SmartEssBleProvisionResult(
                branch=SmartEssBleProvisionBranch.INTPARA,
                outcome=SmartEssBleProvisionOutcome.FAILURE,
                status_code=status_code,
                raw_response=response,
            )

        if requires_restart:
            try:
                response = await self._session.exchange_text(
                    "AT+INTPARA=29,1",
                    timeout=command_timeout,
                    append_crlf=False,
                    response=True,
                )
            except SmartEssBleError as exc:
                if str(exc) != "ble_notification_timeout":
                    raise
                response = ""
            status_code = extract_at_status_code(response, "AT+INTPARA") or "Timeout"
            if response and status_code != "W000":
                return SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    outcome=SmartEssBleProvisionOutcome.FAILURE,
                    status_code=status_code,
                    raw_response=response,
                )

        return await self._poll_intpara48_result()

    async def _poll_link_result(self) -> SmartEssBleProvisionResult:
        last_result: SmartEssBleProvisionResult | None = None
        status_timeout = max(self._status_timeout, _DEFAULT_ANDROID_BLE_TEXT_TIMEOUT)
        for attempt in range(self._max_status_polls):
            if attempt and self._status_poll_interval > 0:
                await asyncio.sleep(self._status_poll_interval)
            try:
                response = await self._session.exchange_text(
                    "AT+LINK?",
                    timeout=status_timeout,
                    append_crlf=False,
                    response=True,
                    drain_before_send=False,
                )
            except SmartEssBleError as exc:
                if str(exc) != "ble_notification_timeout":
                    raise
                continue
            except Exception as exc:
                logger.warning(
                    "SmartESS BLE link-status poll interrupted after apply error=%s",
                    exc,
                )
                return SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    outcome=SmartEssBleProvisionOutcome.DEGRADED,
                    status_code="TransportLost",
                    raw_response="",
                )
            outcome, status_code = parse_link_provision_result(response)
            last_result = SmartEssBleProvisionResult(
                branch=SmartEssBleProvisionBranch.WFLKAP,
                outcome=outcome,
                status_code=status_code,
                raw_response=response,
            )
            if outcome == SmartEssBleProvisionOutcome.SUCCESS:
                return last_result
        if last_result is None:
            return SmartEssBleProvisionResult(
                branch=SmartEssBleProvisionBranch.WFLKAP,
                outcome=SmartEssBleProvisionOutcome.DEGRADED,
                status_code="Timeout",
                raw_response="",
            )
        return last_result

    async def _poll_intpara48_result(self) -> SmartEssBleProvisionResult:
        last_result: SmartEssBleProvisionResult | None = None
        status_timeout = max(self._status_timeout, _DEFAULT_ANDROID_BLE_TEXT_TIMEOUT)
        for attempt in range(self._max_status_polls):
            if attempt and self._status_poll_interval > 0:
                await asyncio.sleep(self._status_poll_interval)
            try:
                response = await self._session.exchange_text(
                    "AT+INTPARA48?",
                    timeout=status_timeout,
                    append_crlf=False,
                    response=True,
                    drain_before_send=False,
                )
            except SmartEssBleError as exc:
                if str(exc) != "ble_notification_timeout":
                    raise
                continue
            except Exception as exc:
                logger.warning(
                    "SmartESS BLE intpara48-status poll interrupted after apply error=%s",
                    exc,
                )
                return SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    outcome=SmartEssBleProvisionOutcome.DEGRADED,
                    status_code="TransportLost",
                    raw_response="",
                    details=None,
                )
            outcome, status_code, details = parse_intpara48_provision_result(response)
            last_result = SmartEssBleProvisionResult(
                branch=SmartEssBleProvisionBranch.INTPARA,
                outcome=outcome,
                status_code=status_code,
                raw_response=response,
                details=details,
            )
            if outcome == SmartEssBleProvisionOutcome.SUCCESS:
                return last_result
        if last_result is None:
            return SmartEssBleProvisionResult(
                branch=SmartEssBleProvisionBranch.INTPARA,
                outcome=SmartEssBleProvisionOutcome.DEGRADED,
                status_code="Timeout",
                raw_response="",
                details=None,
            )
        return last_result

    @staticmethod
    def _validate_wifi_value(value: str, *, error_code: str) -> None:
        text = str(value or "")
        if not text or "\r" in text or "\n" in text:
            raise SmartEssBleError(error_code)
