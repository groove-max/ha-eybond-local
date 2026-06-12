"""Load the offline device identification catalog and match identity fingerprints.

The catalog replaces score-based identity rules with a deterministic lookup over
immutable identity registers. Match semantics:

- ``no_data``     — the identity region read as zeros/absent (inverter comm down);
                    callers must retry/diagnose, never classify the device.
- ``device``      — exact fingerprint match against one catalog entry (full/partial tier).
- ``family``      — layout family recognized but the model is unknown; reads-only
                    family default applies, writes stay locked.
- ``unidentified``— responds to modbus but no cataloged layout matches.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
import os
from pathlib import Path


DEVICE_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "protocol_catalogs" / "device_catalog.json"
)

# DEBUG / VALIDATION toggle. When enabled, exact catalog model matches are
# ignored so every device drops to the family (partial) tier — i.e. it behaves
# as an unsupported model and the learning flow is offered. Use it to exercise
# read + control learning end to end on a device that is otherwise fully
# supported (e.g. an SMG 6200). Flip this constant, or set the environment
# variable EYBOND_FORCE_UNSUPPORTED=1 without editing code. Keep False for
# normal use.
FORCE_UNSUPPORTED_MODELS = False
_FORCE_UNSUPPORTED_ENV = "EYBOND_FORCE_UNSUPPORTED"


def force_unsupported_models() -> bool:
    """Return whether detection must treat every device as an unsupported model."""

    if FORCE_UNSUPPORTED_MODELS:
        return True
    return str(os.environ.get(_FORCE_UNSUPPORTED_ENV, "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

MATCH_NO_DATA = "no_data"
MATCH_DEVICE = "device"
MATCH_FAMILY = "family"
MATCH_UNIDENTIFIED = "unidentified"

TIER_FULL = "full"
TIER_PARTIAL = "partial"

_SERIAL_PLAUSIBLE_MIN_CHARS = 6


@dataclass(frozen=True, slots=True)
class IdentityProbeField:
    """One field extracted from the identity probe register window."""

    register: int
    words: int = 1


@dataclass(frozen=True, slots=True)
class IdentityProbeSpec:
    """Registers to read and fields to extract for one transport family."""

    read_blocks: tuple[tuple[int, int], ...]
    fields: dict[str, IdentityProbeField]


@dataclass(frozen=True, slots=True)
class LayoutFamily:
    """One register-map dialect inside a transport family."""

    key: str
    transport: str
    layout_codes: tuple[int, ...]
    rated_power_register_valid: bool
    base_schema: str


@dataclass(frozen=True, slots=True)
class DeviceFingerprint:
    """Deterministic identity selector for one catalog entry."""

    layout_code: int
    model_code: int
    rated_power_one_of: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class CatalogBinding:
    """Driver/schema/profile binding applied on a match."""

    driver_key: str
    variant_key: str = "default"
    register_schema_name: str = ""
    profile_name: str = ""


@dataclass(frozen=True, slots=True)
class DeviceCatalogEntry:
    """One identified device model in the catalog."""

    entry_key: str
    fingerprint: DeviceFingerprint
    structural: tuple[str, ...]
    model_name: str
    tier: str
    binding: CatalogBinding
    devcodes: tuple[int, ...] = ()
    provenance_sources: tuple[str, ...] = ()
    provenance_confidence: str = ""


@dataclass(frozen=True, slots=True)
class FamilyDefault:
    """Reads-only fallback for a recognized layout with an unknown model."""

    when_layout_codes: tuple[int, ...]
    tier: str
    binding: CatalogBinding
    note: str = ""


@dataclass(frozen=True, slots=True)
class DeviceCatalog:
    """Parsed offline device identification catalog."""

    schema_version: int
    catalog_version: str
    transports: dict[str, IdentityProbeSpec]
    layouts: tuple[LayoutFamily, ...]
    devices: tuple[DeviceCatalogEntry, ...]
    family_defaults: tuple[FamilyDefault, ...]


@dataclass(frozen=True, slots=True)
class DeviceCatalogMatch:
    """Result of matching one identity probe against the catalog."""

    kind: str
    tier: str = ""
    entry: DeviceCatalogEntry | None = None
    layout: LayoutFamily | None = None
    family_default: FamilyDefault | None = None
    confidence_signals: tuple[str, ...] = ()


@lru_cache(maxsize=None)
def load_device_catalog() -> DeviceCatalog:
    """Load and parse the offline device identification catalog."""

    raw = json.loads(DEVICE_CATALOG_PATH.read_text(encoding="utf-8"))
    transports = {
        str(key): _parse_probe_spec(value)
        for key, value in (raw.get("transports") or {}).items()
        if isinstance(value, dict)
    }
    layouts = tuple(
        _parse_layout(item) for item in raw.get("layouts", []) if isinstance(item, dict)
    )
    devices = tuple(
        _parse_device(item) for item in raw.get("devices", []) if isinstance(item, dict)
    )
    family_defaults = tuple(
        _parse_family_default(item)
        for item in raw.get("family_defaults", [])
        if isinstance(item, dict)
    )
    return DeviceCatalog(
        schema_version=int(raw.get("schema_version", 0)),
        catalog_version=str(raw.get("catalog_version", "")),
        transports=transports,
        layouts=layouts,
        devices=devices,
        family_defaults=family_defaults,
    )


def clear_device_catalog_cache() -> None:
    """Clear the cached device identification catalog."""

    load_device_catalog.cache_clear()


def serial_ascii_plausible(serial_ascii: str) -> bool:
    """Check that a decoded serial block looks like a real ASCII serial.

    Anonymized captures scramble the serial words, so this check only ever ADDS
    a confidence signal; an implausible serial must never reject a match.
    """

    cleaned = "".join(
        char for char in str(serial_ascii or "") if char.isprintable() and char.isalnum()
    )
    return len(cleaned) >= _SERIAL_PLAUSIBLE_MIN_CHARS


def match_device_identity(
    *,
    layout_code: int | None,
    model_code: int | None,
    rated_power: int | None = None,
    serial_ascii: str = "",
    catalog: DeviceCatalog | None = None,
) -> DeviceCatalogMatch:
    """Match one identity probe result against the catalog.

    ``rated_power`` narrowing applies only when the value was actually read: an
    entry that pins ``rated_power_one_of`` rejects a DIFFERENT wattage but still
    matches when the register was not captured.
    """

    resolved = catalog if catalog is not None else load_device_catalog()

    if layout_code is None or model_code is None:
        return DeviceCatalogMatch(kind=MATCH_NO_DATA)
    if layout_code == 0 and model_code == 0:
        # The identity region reads as zeros when the collector has no inverter
        # link; classifying this as "unknown device" caused real false negatives.
        return DeviceCatalogMatch(kind=MATCH_NO_DATA)

    layout = _resolve_layout(resolved, layout_code)

    # Debug toggle: skip exact model matching so a supported device drops to the
    # family/partial tier and the learning flow can be validated on it.
    if not force_unsupported_models():
        for entry in resolved.devices:
            fingerprint = entry.fingerprint
            if fingerprint.layout_code != layout_code:
                continue
            if fingerprint.model_code != model_code:
                continue
            if (
                fingerprint.rated_power_one_of
                and rated_power is not None
                and rated_power not in fingerprint.rated_power_one_of
            ):
                continue
            signals = _confidence_signals(
                entry=entry,
                rated_power=rated_power,
                serial_ascii=serial_ascii,
            )
            return DeviceCatalogMatch(
                kind=MATCH_DEVICE,
                tier=entry.tier,
                entry=entry,
                layout=layout,
                confidence_signals=signals,
            )

    if layout is not None:
        for default in resolved.family_defaults:
            if layout_code in default.when_layout_codes:
                return DeviceCatalogMatch(
                    kind=MATCH_FAMILY,
                    tier=default.tier,
                    layout=layout,
                    family_default=default,
                    confidence_signals=("layout_code",),
                )

    return DeviceCatalogMatch(kind=MATCH_UNIDENTIFIED, layout=layout)


def resolve_family_default(
    layout_code: int,
    *,
    catalog: DeviceCatalog | None = None,
) -> FamilyDefault | None:
    """Resolve the reads-only family default for one layout code, if any."""

    resolved = catalog if catalog is not None else load_device_catalog()
    for default in resolved.family_defaults:
        if layout_code in default.when_layout_codes:
            return default
    return None


def _confidence_signals(
    *,
    entry: DeviceCatalogEntry,
    rated_power: int | None,
    serial_ascii: str,
) -> tuple[str, ...]:
    signals = ["layout_code", "model_code"]
    if (
        entry.fingerprint.rated_power_one_of
        and rated_power is not None
        and rated_power in entry.fingerprint.rated_power_one_of
    ):
        signals.append("rated_power")
    if "serial_ascii_plausible" in entry.structural and serial_ascii_plausible(serial_ascii):
        signals.append("serial_ascii")
    return tuple(signals)


def _resolve_layout(catalog: DeviceCatalog, layout_code: int) -> LayoutFamily | None:
    for layout in catalog.layouts:
        if layout_code in layout.layout_codes:
            return layout
    return None


def _parse_probe_spec(raw: dict[str, object]) -> IdentityProbeSpec:
    probe = raw.get("identity_probe")
    probe = probe if isinstance(probe, dict) else {}
    read_blocks = tuple(
        (int(block[0]), int(block[1]))
        for block in probe.get("read_blocks", [])
        if isinstance(block, (list, tuple)) and len(block) == 2
    )
    fields: dict[str, IdentityProbeField] = {}
    raw_fields = probe.get("fields")
    if isinstance(raw_fields, dict):
        for name, value in raw_fields.items():
            if isinstance(value, dict) and "register" in value:
                fields[str(name)] = IdentityProbeField(
                    register=int(value["register"]),
                    words=int(value.get("words", 1)),
                )
    return IdentityProbeSpec(read_blocks=read_blocks, fields=fields)


def _parse_layout(raw: dict[str, object]) -> LayoutFamily:
    return LayoutFamily(
        key=str(raw["key"]).strip(),
        transport=str(raw.get("transport", "")).strip(),
        layout_codes=tuple(int(code) for code in raw.get("layout_codes", [])),
        rated_power_register_valid=bool(raw.get("rated_power_register_valid", False)),
        base_schema=str(raw.get("base_schema", "")).strip(),
    )


def _parse_fingerprint(raw: dict[str, object]) -> DeviceFingerprint:
    return DeviceFingerprint(
        layout_code=int(raw["layout_code"]),
        model_code=int(raw["model_code"]),
        rated_power_one_of=tuple(int(value) for value in raw.get("rated_power_one_of", [])),
    )


def _parse_binding(raw: dict[str, object]) -> CatalogBinding:
    return CatalogBinding(
        driver_key=str(raw["driver_key"]).strip(),
        variant_key=str(raw.get("variant_key", "default")).strip() or "default",
        register_schema_name=str(raw.get("register_schema_name", "")).strip(),
        profile_name=str(raw.get("profile_name", "")).strip(),
    )


def _parse_device(raw: dict[str, object]) -> DeviceCatalogEntry:
    provenance = raw.get("provenance")
    provenance = provenance if isinstance(provenance, dict) else {}
    cloud_hints = raw.get("cloud_hints")
    cloud_hints = cloud_hints if isinstance(cloud_hints, dict) else {}
    return DeviceCatalogEntry(
        entry_key=str(raw["entry_key"]).strip(),
        fingerprint=_parse_fingerprint(raw["fingerprint"]),
        structural=tuple(str(check) for check in raw.get("structural", [])),
        model_name=str(raw.get("model_name", "")).strip(),
        tier=str(raw.get("tier", "")).strip(),
        binding=_parse_binding(raw["binding"]),
        devcodes=tuple(int(code) for code in cloud_hints.get("devcodes", [])),
        provenance_sources=tuple(str(item) for item in provenance.get("sources", [])),
        provenance_confidence=str(provenance.get("confidence", "")).strip(),
    )


def _parse_family_default(raw: dict[str, object]) -> FamilyDefault:
    return FamilyDefault(
        when_layout_codes=tuple(int(code) for code in raw.get("when_layout_codes", [])),
        tier=str(raw.get("tier", "")).strip(),
        binding=_parse_binding(raw["binding"]),
        note=str(raw.get("note", "")).strip(),
    )
