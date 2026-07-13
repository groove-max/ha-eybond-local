"""SmartESS protocol-descriptor resolution over the neutral collector wire.

The FC=2/FC=3 collector-management wire itself is provider-neutral and now lives
in ``collector_wire``. This module keeps the SmartESS-specific protocol-asset
catalog resolution (query 14 -> asset id) and re-exports the neutral wire names
for backward compatibility. ``SmartEssLocalSession`` is the neutral wire session
plus the SmartESS descriptor/catalog reads.
"""

from __future__ import annotations

from dataclasses import dataclass

from .collector_wire import (
    CollectorManagementUnsupportedError,
    CollectorQueryResponse,
    CollectorSetResponse,
    CollectorWireError,
    CollectorWireManagementSession,
    QUERY_COLLECTOR_PN,
    QUERY_COLLECTOR_VERSION,
    QUERY_HARDWARE_VERSION,
    QUERY_NETWORK_DIAGNOSTICS,
    QUERY_PROTOCOL_DESCRIPTOR,
    QUERY_REBOOT_REQUIRED,
    QUERY_SERIAL_BAUDRATE,
    QUERY_WIFI_SCAN_LIST,
    SET_REBOOT_OR_APPLY,
    SET_SERIAL_BAUDRATE,
    SET_SERVER_ENDPOINT,
    SET_TARGET_PASSWORD,
    SET_TARGET_SSID,
    async_send_collector_reboot_or_apply,
    build_query_collector_payload,
    build_set_collector_payload,
    parse_query_collector_response,
    parse_set_collector_response,
)
from ..metadata.smartess_protocol_catalog_loader import (
    SmartEssProtocolCatalogEntry,
    load_smartess_protocol_catalog,
)

# ``SmartEssLocalError`` is the historical name of the neutral wire error; keep
# it as an alias so existing callers/tests keep working.
SmartEssLocalError = CollectorWireError

_LEGACY_PROTOCOL_ASSET_ALIASES: dict[str, str] = {
    "0230": "0942",
}


@dataclass(frozen=True, slots=True)
class SmartEssProtocolDescriptor:
    """Protocol asset descriptor decoded from query parameter 14."""

    raw_id: str
    asset_id: str
    asset_name: str
    suffix: str = ""
    uses_legacy_alias: bool = False


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

    # Some collectors answer query 14 with a composite serial-protocol config
    # string ("02FF,0,0,#0#"); the protocol id is the first comma field.
    raw_id = raw_id.split(",", 1)[0].strip()
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


class SmartEssLocalSession(CollectorWireManagementSession):
    """Neutral collector wire session plus SmartESS protocol-descriptor reads."""

    async def query_protocol_descriptor(self) -> SmartEssProtocolDescriptor:
        """Read and parse the SmartESS protocol asset descriptor using query 14."""

        response = await self.query_collector(QUERY_PROTOCOL_DESCRIPTOR)
        if response.code != 0:
            raise SmartEssLocalError(
                f"query_failed:parameter={response.parameter}:code={response.code}"
            )
        return resolve_protocol_descriptor(response)

    async def query_known_protocol(self) -> SmartEssProtocolCatalogEntry | None:
        """Read query 14 and resolve it against the built-in SmartESS protocol catalog."""

        descriptor = await self.query_protocol_descriptor()
        return load_smartess_protocol_catalog().protocols.get(descriptor.asset_id)
