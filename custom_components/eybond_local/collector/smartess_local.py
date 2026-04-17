"""SmartESS local collector helpers over the existing reverse TCP channel."""

from __future__ import annotations

from dataclasses import dataclass

from .protocol import FC_QUERY_COLLECTOR, FC_SET_COLLECTOR
from .transport import CollectorTransport
from ..metadata.smartess_protocol_catalog_loader import SmartEssProtocolCatalogEntry, load_smartess_protocol_catalog

QUERY_COLLECTOR_VERSION = 5
QUERY_PROTOCOL_DESCRIPTOR = 14
QUERY_REBOOT_REQUIRED = 30
QUERY_NETWORK_DIAGNOSTICS = 48
QUERY_WIFI_SCAN_LIST = 49

SET_REBOOT_OR_APPLY = 29
SET_TARGET_SSID = 41
SET_TARGET_PASSWORD = 43

_LEGACY_PROTOCOL_ASSET_ALIASES: dict[str, str] = {
    "0230": "0942",
}


class SmartEssLocalError(Exception):
    """Raised when one SmartESS local collector payload is invalid."""


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


@dataclass(frozen=True, slots=True)
class SmartEssProtocolDescriptor:
    """Protocol asset descriptor decoded from query parameter 14."""

    raw_id: str
    asset_id: str
    asset_name: str
    suffix: str = ""
    uses_legacy_alias: bool = False


def build_query_collector_payload(*parameters: int) -> bytes:
    """Build one FC=2 collector query payload from one or more parameters."""

    if not parameters:
        raise SmartEssLocalError("query_parameters_required")
    return bytes(_coerce_u8(parameter, label="query_parameter") for parameter in parameters)


def parse_query_collector_response(payload: bytes) -> CollectorQueryResponse:
    """Decode one FC=2 collector response payload."""

    if len(payload) < 2:
        raise SmartEssLocalError("query_response_too_short")

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
        raise SmartEssLocalError("set_value_not_ascii")
    return bytes((parameter_u8,)) + value.encode("ascii")


def parse_set_collector_response(payload: bytes) -> CollectorSetResponse:
    """Decode one FC=3 collector response payload."""

    if len(payload) < 2:
        raise SmartEssLocalError("set_response_too_short")
    return CollectorSetResponse(status=payload[0], parameter=payload[1])


def resolve_protocol_descriptor(
    value: CollectorQueryResponse | str | bytes,
) -> SmartEssProtocolDescriptor:
    """Resolve the SmartESS protocol asset descriptor returned by query 14."""

    if isinstance(value, CollectorQueryResponse):
        text = value.text
    elif isinstance(value, bytes):
        text = value.decode("ascii", errors="ignore")
    else:
        text = str(value)

    descriptor = text.strip().strip("\x00")
    if not descriptor:
        raise SmartEssLocalError("protocol_descriptor_empty")

    raw_id, _, suffix = descriptor.partition("#")
    raw_id = raw_id.strip()
    if not raw_id:
        raise SmartEssLocalError("protocol_descriptor_missing_id")

    asset_id = _LEGACY_PROTOCOL_ASSET_ALIASES.get(raw_id, raw_id)
    return SmartEssProtocolDescriptor(
        raw_id=raw_id,
        asset_id=asset_id,
        asset_name=f"{asset_id}.json",
        suffix=suffix.strip(),
        uses_legacy_alias=asset_id != raw_id,
    )


class SmartEssLocalSession:
    """SmartESS local collector session over the shared EyeBond reverse TCP transport."""

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

    async def query_collector_version(self) -> str:
        """Read the collector version string using query parameter 5."""

        response = await self.query_collector(QUERY_COLLECTOR_VERSION)
        _require_query_success(response)
        return response.text

    async def query_protocol_descriptor(self) -> SmartEssProtocolDescriptor:
        """Read and parse the SmartESS protocol asset descriptor using query 14."""

        response = await self.query_collector(QUERY_PROTOCOL_DESCRIPTOR)
        _require_query_success(response)
        return resolve_protocol_descriptor(response)

    async def query_known_protocol(self) -> SmartEssProtocolCatalogEntry | None:
        """Read query 14 and resolve it against the built-in SmartESS protocol catalog."""

        descriptor = await self.query_protocol_descriptor()
        return load_smartess_protocol_catalog().protocols.get(descriptor.asset_id)


def _require_query_success(response: CollectorQueryResponse) -> None:
    if response.code != 0:
        raise SmartEssLocalError(
            f"query_failed:parameter={response.parameter}:code={response.code}"
        )


def _coerce_u8(value: int, *, label: str) -> int:
    numeric = int(value)
    if not 0 <= numeric <= 0xFF:
        raise SmartEssLocalError(f"{label}_out_of_range")
    return numeric


def _coerce_u16(value: int, *, label: str) -> int:
    numeric = int(value)
    if not 0 <= numeric <= 0xFFFF:
        raise SmartEssLocalError(f"{label}_out_of_range")
    return numeric