"""Provider-neutral EyeBond collector-management wire primitives.

FC=2 (query) / FC=3 (set) is the EyeBond collector-management wire protocol, not
a SmartESS cloud/catalog service. This module owns the neutral wire session and
the low-level payload build/parse so a provider-neutral collector-management
adapter can use them without importing SmartESS-specific naming.

The SmartESS-specific protocol-descriptor/catalog resolution stays in
``smartess_local`` (which re-exports these neutral names for compatibility).
"""

from __future__ import annotations

from dataclasses import dataclass

from .protocol import FC_QUERY_COLLECTOR, FC_SET_COLLECTOR
from .transport import CollectorTransport

# FC=2 read parameters.
QUERY_COLLECTOR_PN = 2
QUERY_COLLECTOR_VERSION = 5
QUERY_HARDWARE_VERSION = 6
QUERY_PROTOCOL_DESCRIPTOR = 14
QUERY_REBOOT_REQUIRED = 30
QUERY_SERIAL_BAUDRATE = 34
QUERY_NETWORK_DIAGNOSTICS = 48
QUERY_WIFI_SCAN_LIST = 49

# FC=3 set parameters.
SET_SERVER_ENDPOINT = 21
SET_REBOOT_OR_APPLY = 29
SET_SERIAL_BAUDRATE = 34
SET_TARGET_SSID = 41
SET_TARGET_PASSWORD = 43


class CollectorWireError(Exception):
    """Raised when one collector-management wire payload is invalid."""


class CollectorManagementError(RuntimeError):
    """Base for every collector-management operation failure.

    Defined at the wire layer so both the neutral wire helpers and the
    provider-neutral management adapters share one exception hierarchy.
    """


class CollectorManagementUnsupportedError(CollectorManagementError):
    """The transport cannot carry collector-management commands at all.

    Typed so callers (runtime entity paths and onboarding strategy
    verification) can distinguish "this collector/session cannot be managed"
    from "the command was sent but not confirmed" without matching substrings.
    """


@dataclass(frozen=True, slots=True)
class CollectorQueryResponse:
    """Decoded FC=2 collector response payload."""

    code: int
    parameter: int
    data: bytes
    text: str


@dataclass(frozen=True, slots=True)
class CollectorSetResponse:
    """Decoded FC=3 collector response payload."""

    status: int
    parameter: int


def build_query_collector_payload(*parameters: int) -> bytes:
    """Build one FC=2 collector query payload from one or more parameters."""

    if not parameters:
        raise CollectorWireError("query_parameters_required")
    return bytes(_coerce_u8(parameter, label="query_parameter") for parameter in parameters)


def parse_query_collector_response(payload: bytes) -> CollectorQueryResponse:
    """Decode one FC=2 collector response payload."""

    if len(payload) < 2:
        raise CollectorWireError("query_response_too_short")

    data = bytes(payload[2:])
    return CollectorQueryResponse(
        code=payload[0],
        parameter=payload[1],
        data=data,
        text=data.decode("ascii", errors="ignore").strip("\x00"),
    )


def build_set_collector_payload(parameter: int, value: str) -> bytes:
    """Build one FC=3 collector set payload."""

    parameter_u8 = _coerce_u8(parameter, label="set_parameter")
    if not isinstance(value, str) or not value.isascii():
        raise CollectorWireError("set_value_not_ascii")
    return bytes((parameter_u8,)) + value.encode("ascii")


def parse_set_collector_response(payload: bytes) -> CollectorSetResponse:
    """Decode one FC=3 collector response payload."""

    if len(payload) < 2:
        raise CollectorWireError("set_response_too_short")
    return CollectorSetResponse(status=payload[0], parameter=payload[1])


class CollectorWireManagementSession:
    """Neutral FC=2/FC=3 collector-management session over the reverse TCP transport."""

    def __init__(
        self,
        transport: CollectorTransport,
        *,
        devcode: int = 1,
        collector_addr: int = 1,
    ) -> None:
        self._transport = transport
        self._devcode = _coerce_u16(devcode, label="devcode")
        self._collector_addr = _coerce_u8(collector_addr, label="collector_addr")

    async def query_collector(self, *parameters: int) -> CollectorQueryResponse:
        """Send one collector FC=2 query and decode the response payload."""

        _, payload = await self._transport.async_send_collector(
            fcode=FC_QUERY_COLLECTOR,
            payload=build_query_collector_payload(*parameters),
            devcode=self._devcode,
            collector_addr=self._collector_addr,
        )
        return parse_query_collector_response(payload)

    async def set_collector(self, parameter: int, value: str) -> CollectorSetResponse:
        """Send one collector FC=3 set request and decode the response payload."""

        _, payload = await self._transport.async_send_collector(
            fcode=FC_SET_COLLECTOR,
            payload=build_set_collector_payload(parameter, value),
            devcode=self._devcode,
            collector_addr=self._collector_addr,
        )
        return parse_set_collector_response(payload)

    async def query_collector_text(self, parameter: int) -> str:
        """Read one collector parameter as trimmed ASCII text, "" on failure."""

        response = await self.query_collector(parameter)
        if response.code != 0:
            return ""
        return str(response.text or "").strip().strip("\x00")

    async def query_collector_version(self) -> str:
        """Read the collector version string using query parameter 5."""

        response = await self.query_collector(QUERY_COLLECTOR_VERSION)
        _require_query_success(response)
        return response.text

    async def query_collector_pn(self) -> str:
        """Read the collector's authoritative full PN using FC=2 parameter 2."""

        response = await self.query_collector(QUERY_COLLECTOR_PN)
        _require_query_success(response)
        if not response.text:
            raise CollectorWireError("collector_pn_empty")
        return response.text


async def async_send_collector_reboot_or_apply(transport: CollectorTransport) -> None:
    """Send the single collector reboot/apply wire command (parameter 29 = "1").

    THE low-level restart command. Both the runtime collector-management adapter
    and onboarding connection-strategy verification use it, so the wire command
    exists exactly once. Raises when the transport cannot carry collector
    management or the collector did not confirm the set.
    """

    if not hasattr(transport, "async_send_collector"):
        raise CollectorManagementUnsupportedError(
            "collector_local_management_not_supported"
        )

    session = CollectorWireManagementSession(transport)
    response = await session.set_collector(SET_REBOOT_OR_APPLY, "1")
    if response.status != 0 or response.parameter != SET_REBOOT_OR_APPLY:
        raise RuntimeError(
            f"collector_set_failed:parameter={SET_REBOOT_OR_APPLY}:status={response.status}"
        )


def _require_query_success(response: CollectorQueryResponse) -> None:
    if response.code != 0:
        raise CollectorWireError(
            f"query_failed:parameter={response.parameter}:code={response.code}"
        )


def _coerce_u8(value: int, *, label: str) -> int:
    numeric = int(value)
    if not 0 <= numeric <= 0xFF:
        raise CollectorWireError(f"{label}_out_of_range")
    return numeric


def _coerce_u16(value: int, *, label: str) -> int:
    numeric = int(value)
    if not 0 <= numeric <= 0xFFFF:
        raise CollectorWireError(f"{label}_out_of_range")
    return numeric
