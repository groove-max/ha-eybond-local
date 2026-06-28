"""Driver-specific diagnostic command targets.

Each adapter exposes ONLY the raw read/write/ascii primitives that a driver's
transport supports, deliberately bypassing the capability/profile layer because
a diagnosed register may not exist in any profile. The command-vs-driver support
matrix lives here as ``DIAGNOSTIC_SUPPORTED_KINDS`` (one source of truth), not
spread across the parser.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..link_transport import PayloadLinkTransport, async_send_payload, select_payload_route
from ..models import ProbeTarget
from ..payload import ascii_line
from ..payload import pi18 as pi18_payload
from ..payload import pi30 as pi30_payload
from ..payload.modbus import ModbusSession, merge_register_bit


class DiagnosticTargetError(Exception):
    """Raised when a diagnostic primitive fails in a non-transport way."""


class UnsupportedDriverError(DiagnosticTargetError):
    """Raised when no diagnostic target exists for a driver key."""


# Single source of truth for the command/driver compatibility matrix.
DIAGNOSTIC_SUPPORTED_KINDS: dict[str, frozenset[str]] = {
    "modbus_smg": frozenset({"read", "write", "write_bit"}),
    "pi30": frozenset({"ascii"}),
    "pi18": frozenset({"ascii"}),
    "eybond_g_ascii": frozenset({"ascii"}),
}


def supported_kinds_for_driver(driver_key: str) -> frozenset[str]:
    """Return the diagnostic primitives one driver supports (empty if unknown)."""

    return DIAGNOSTIC_SUPPORTED_KINDS.get(driver_key, frozenset())


def has_diagnostic_target(driver_key: str) -> bool:
    return driver_key in DIAGNOSTIC_SUPPORTED_KINDS


@dataclass(frozen=True, slots=True)
class WriteBitOutcome:
    register: int
    bit_index: int
    bit_value: int
    mask: int
    before: int
    written: int


@dataclass(frozen=True, slots=True)
class AsciiOutcome:
    raw: bytes
    payload: str | None
    decode_error: str | None


class ModbusDiagnosticTarget:
    """Raw Modbus holding-register primitives over the shared collector link."""

    supported_kinds = frozenset({"read", "write", "write_bit"})

    def __init__(self, driver_key: str, session: ModbusSession) -> None:
        self.driver_key = driver_key
        self._session = session

    async def read_holding(self, register: int, count: int) -> list[int]:
        return await self._session.read_holding(register, count)

    async def write_holding(self, register: int, values: list[int]) -> None:
        await self._session.write_holding(register, list(values))

    async def write_bit(
        self, register: int, bit_index: int, bit_value: int
    ) -> WriteBitOutcome:
        # The pre-read IS the read-modify-write, not an extra safety check. If it
        # fails or returns nothing, no write happens (we never reach write_holding).
        current = await self._session.read_holding(register, 1)
        if not current:
            raise DiagnosticTargetError("write_bit_pre_read_empty")
        before = int(current[0]) & 0xFFFF
        mask = 1 << bit_index
        merged = merge_register_bit(before, bit_index, bit_value)
        await self._session.write_holding(register, [merged])
        return WriteBitOutcome(
            register=register,
            bit_index=bit_index,
            bit_value=bit_value,
            mask=mask,
            before=before,
            written=merged,
        )


class AsciiDiagnosticTarget:
    """Raw ASCII command primitive over the shared collector link (PI drivers)."""

    supported_kinds = frozenset({"ascii"})

    def __init__(
        self,
        driver_key: str,
        transport: PayloadLinkTransport,
        route: object,
        build_request,
        parse_response,
        decode_errors: tuple[type[Exception], ...],
        payload_family: str | None = None,
    ) -> None:
        self.driver_key = driver_key
        self._transport = transport
        self._route = route
        self._build_request = build_request
        self._parse_response = parse_response
        self._decode_errors = decode_errors
        self._payload_family = payload_family or f"{driver_key}_ascii"

    async def send_ascii(
        self,
        command: str,
        *,
        request_timeout: float | None = None,
    ) -> AsciiOutcome:
        request = self._build_request(command)
        route = select_payload_route(
            self._transport,
            self._route,
            payload_family=self._payload_family,
        )
        raw = await async_send_payload(
            self._transport,
            request,
            route=route,
            request_timeout=request_timeout,
        )
        payload: str | None = None
        decode_error: str | None = None
        try:
            # parse_response strips framing/CRC and returns the payload. A device
            # NAK ("NAK"/"NOA"/"ERCRC") decodes to a normal payload here (only the
            # high-level driver request() raises on it), so diagnostics surface it
            # as data rather than an error.
            payload = self._parse_response(raw)
        except self._decode_errors as exc:
            decode_error = str(exc)
        return AsciiOutcome(raw=raw, payload=payload, decode_error=decode_error)


def build_diagnostic_target(
    driver_key: str,
    transport: PayloadLinkTransport,
    target: ProbeTarget,
):
    """Build the diagnostic target adapter for one driver (no transport I/O)."""

    if driver_key == "modbus_smg":
        session = ModbusSession(
            transport,
            route=target.link_route,
            slave_id=target.payload_address,
        )
        return ModbusDiagnosticTarget(driver_key, session)
    if driver_key == "pi30":
        return AsciiDiagnosticTarget(
            driver_key,
            transport,
            target.link_route,
            pi30_payload.build_request,
            pi30_payload.parse_response,
            (pi30_payload.Pi30Error,),
        )
    if driver_key == "pi18":
        return AsciiDiagnosticTarget(
            driver_key,
            transport,
            target.link_route,
            pi18_payload.build_request,
            pi18_payload.parse_response,
            (pi18_payload.Pi18Error,),
        )
    if driver_key == "eybond_g_ascii":
        return AsciiDiagnosticTarget(
            driver_key,
            transport,
            target.link_route,
            ascii_line.build_ascii_line_request,
            ascii_line.parse_ascii_line_response,
            (ascii_line.AsciiLineError,),
            payload_family="eybond_g_ascii",
        )
    raise UnsupportedDriverError(f"no_diagnostic_target_for_driver:{driver_key}")
