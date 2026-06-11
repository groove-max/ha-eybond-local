"""Catalog identity probe over the immutable identity registers.

Reads the device-catalog identity window (see ``protocol_catalogs/device_catalog.json``)
through one modbus session and matches the result against the offline catalog.
Runs in SHADOW MODE during detection: the legacy identity rules still decide the
binding while catalog agreement/disagreement is logged and recorded into probe
details for support packages.

The one behavioral change is the no-data rule: an identity region that reads as
zeros means the collector currently has no inverter link, so detection surfaces
``inverter_link_down`` instead of misreporting an unsupported device.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

from ..metadata.device_catalog_loader import (
    MATCH_NO_DATA,
    DeviceCatalogMatch,
    load_device_catalog,
    match_device_identity,
)

logger = logging.getLogger(__name__)

ERROR_INVERTER_LINK_DOWN = "inverter_link_down"

DEFAULT_TRANSPORT_KEY = "eybond_modbus"


class InverterIdentityNoDataError(RuntimeError):
    """The identity register region read as zeros: inverter link is down."""

    def __init__(self) -> None:
        super().__init__(ERROR_INVERTER_LINK_DOWN)


@dataclass(frozen=True, slots=True)
class CatalogIdentityProbe:
    """Raw identity fields plus their catalog match."""

    layout_code: int | None
    model_code: int | None
    rated_power: int | None
    serial_ascii: str
    match: DeviceCatalogMatch

    def as_details(self) -> dict[str, Any]:
        """Serialize for DetectedInverter.details / support diagnostics."""

        payload: dict[str, Any] = {
            "kind": self.match.kind,
            "layout_code": self.layout_code,
            "model_code": self.model_code,
            "rated_power": self.rated_power,
            "confidence_signals": list(self.match.confidence_signals),
        }
        if self.match.entry is not None:
            payload["entry_key"] = self.match.entry.entry_key
            payload["tier"] = self.match.entry.tier
            payload["catalog_variant_key"] = self.match.entry.binding.variant_key
        elif self.match.kind:
            payload["tier"] = self.match.tier
        if self.match.layout is not None:
            payload["layout_key"] = self.match.layout.key
        return payload


async def async_probe_catalog_identity(
    session: Any,
    *,
    transport_key: str = DEFAULT_TRANSPORT_KEY,
) -> CatalogIdentityProbe | None:
    """Read the identity window and match it against the device catalog.

    Returns ``None`` when no identity block could be read at all (a non-modbus
    device, or transport failure) — callers must treat that as "no opinion",
    NOT as link-down.
    """

    catalog = load_device_catalog()
    spec = catalog.transports.get(transport_key)
    if spec is None:
        return None

    words: dict[int, int] = {}
    any_block_read = False
    for start, count in spec.read_blocks:
        try:
            values = await session.read_holding(start, count)
        except Exception as exc:  # per-block tolerance: identity must not hard-fail probing
            logger.debug("Catalog identity block read failed start=%s error=%s", start, exc)
            continue
        any_block_read = True
        for index, value in enumerate(values):
            words[start + index] = int(value)
    if not any_block_read:
        return None

    layout_code = _field_value(spec, words, "layout_code")
    model_code = _field_value(spec, words, "model_code")
    if layout_code is None or model_code is None:
        # The fingerprint registers themselves were unreadable: that is "no
        # opinion" (e.g. a layout that rejects this block read), NOT link-down.
        # Link-down is specifically a SUCCESSFUL read returning zeros.
        return None
    rated_power = _field_value(spec, words, "rated_power")
    serial_ascii = _decode_serial(spec, words)

    match = match_device_identity(
        layout_code=layout_code,
        model_code=model_code,
        rated_power=rated_power,
        serial_ascii=serial_ascii,
        catalog=catalog,
    )
    return CatalogIdentityProbe(
        layout_code=layout_code,
        model_code=model_code,
        rated_power=rated_power,
        serial_ascii=serial_ascii,
        match=match,
    )


def probe_indicates_link_down(probe: CatalogIdentityProbe | None) -> bool:
    """True when identity registers were READ but came back as zeros."""

    return probe is not None and probe.match.kind == MATCH_NO_DATA


def attach_catalog_match_details(detected: Any, probe: CatalogIdentityProbe | None) -> None:
    """Attach the catalog decision to a probe result for diagnostics.

    The dict lands in runtime values and therefore in support packages, so a
    user report always shows WHY the device got its tier/binding.
    """

    if probe is None:
        return
    details = getattr(detected, "details", None)
    if isinstance(details, dict):
        details["device_catalog"] = probe.as_details()
    logger.debug(
        "Device catalog decision: kind=%s entry=%s variant=%s layout_code=%s "
        "model_code=%s rated_power=%s",
        probe.match.kind,
        probe.match.entry.entry_key if probe.match.entry is not None else None,
        str(getattr(detected, "variant_key", "")),
        probe.layout_code,
        probe.model_code,
        probe.rated_power,
    )


def _field_value(spec: Any, words: dict[str, int] | dict[int, int], name: str) -> int | None:
    field = spec.fields.get(name)
    if field is None:
        return None
    return words.get(field.register)


def _decode_serial(spec: Any, words: dict[int, int]) -> str:
    field = spec.fields.get("serial_ascii")
    if field is None:
        return ""
    raw = bytearray()
    for offset in range(field.words):
        value = words.get(field.register + offset)
        if value is None:
            continue
        raw += int(value).to_bytes(2, "big")
    text = raw.decode("ascii", errors="replace")
    return "".join(char for char in text if char.isprintable() and char.isalnum())
