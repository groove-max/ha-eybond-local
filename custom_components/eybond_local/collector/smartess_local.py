"""SmartESS protocol-descriptor resolution over the neutral collector wire.

The FC=2/FC=3 collector-management wire itself is provider-neutral and lives in
``collector_wire``. This module owns only SmartESS-specific protocol-asset
catalog resolution (query 14 -> asset id) and the specialized session facade.
"""

from __future__ import annotations

from dataclasses import dataclass

from .collector_wire import (
    CollectorQueryResponse as _CollectorQueryResponse,
    CollectorWireError as _CollectorWireError,
    CollectorWireManagementSession as _CollectorWireManagementSession,
    QUERY_PROTOCOL_DESCRIPTOR as _QUERY_PROTOCOL_DESCRIPTOR,
)
from ..metadata.smartess_protocol_catalog_loader import (
    SmartEssProtocolCatalogEntry,
    load_smartess_protocol_catalog,
)

class SmartEssProtocolError(_CollectorWireError):
    """A SmartESS protocol descriptor cannot be resolved."""

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
    value: _CollectorQueryResponse | str | bytes,
) -> SmartEssProtocolDescriptor:
    """Resolve the SmartESS protocol asset descriptor returned by query 14."""

    if isinstance(value, _CollectorQueryResponse):
        text = value.text
    elif isinstance(value, bytes):
        text = value.decode("ascii", errors="ignore")
    else:
        text = str(value)

    descriptor = text.strip().strip("\x00")
    if not descriptor:
        raise SmartEssProtocolError("protocol_descriptor_empty")

    raw_id, _, suffix = descriptor.partition("#")
    raw_id = raw_id.strip()
    if not raw_id:
        raise SmartEssProtocolError("protocol_descriptor_missing_id")

    # Some collectors answer query 14 with a composite serial-protocol config
    # string ("02FF,0,0,#0#"); the protocol id is the first comma field.
    raw_id = raw_id.split(",", 1)[0].strip()
    if not raw_id:
        raise SmartEssProtocolError("protocol_descriptor_missing_id")

    asset_id = _LEGACY_PROTOCOL_ASSET_ALIASES.get(raw_id, raw_id)
    return SmartEssProtocolDescriptor(
        raw_id=raw_id,
        asset_id=asset_id,
        asset_name=f"{asset_id}.json",
        suffix=suffix.strip(),
        uses_legacy_alias=asset_id != raw_id,
    )


class SmartEssLocalSession(_CollectorWireManagementSession):
    """Neutral collector wire session plus SmartESS protocol-descriptor reads."""

    async def query_protocol_descriptor(self) -> SmartEssProtocolDescriptor:
        """Read and parse the SmartESS protocol asset descriptor using query 14."""

        response = await self.query_collector(_QUERY_PROTOCOL_DESCRIPTOR)
        if response.code != 0:
            raise SmartEssProtocolError(
                f"query_failed:parameter={response.parameter}:code={response.code}"
            )
        return resolve_protocol_descriptor(response)

    async def query_known_protocol(self) -> SmartEssProtocolCatalogEntry | None:
        """Read query 14 and resolve it against the built-in SmartESS protocol catalog."""

        descriptor = await self.query_protocol_descriptor()
        return load_smartess_protocol_catalog().protocols.get(descriptor.asset_id)
