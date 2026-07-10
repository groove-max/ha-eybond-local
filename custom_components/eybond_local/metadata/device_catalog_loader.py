"""Load the universal offline inverter catalog.

Compiled runtime detection owns model and surface resolution. The compatibility
match types below are retained only for serialized diagnostics:

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
    Path(__file__).resolve().parents[1] / "protocol_catalogs" / "inverter_catalog.json"
)

# DEBUG / VALIDATION toggle. When enabled, exact catalog model matches are
# ignored so every device drops to the family (partial) tier — i.e. it behaves
# as an unsupported model and the learning flow is offered. Use it to exercise
# read + control learning end to end on a device that is otherwise fully
# supported (e.g. an SMG 6200).
#
# It is enabled by EITHER the EYBOND_FORCE_UNSUPPORTED environment variable OR
# the on-device force_unsupported.flag sentinel (see FORCE_UNSUPPORTED_SENTINEL
# _NAME below) — and is deliberately NOT a hard-coded constant. A hard-coded
# `True` once reached a production device through an rsync of the working tree,
# forced every model to the partial tier, and the resulting entity +
# recorder-write explosion OOM-looped the host. Neither the env var nor the
# config-dir sentinel travels with the source tree, so the toggle cannot ship by
# accident. To test locally: `EYBOND_FORCE_UNSUPPORTED=1`.
_FORCE_UNSUPPORTED_ENV = "EYBOND_FORCE_UNSUPPORTED"


def _env_force_unsupported() -> bool:
    return str(os.environ.get(_FORCE_UNSUPPORTED_ENV, "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


# Resolved once at import from the environment only (never a tracked literal).
FORCE_UNSUPPORTED_MODELS = _env_force_unsupported()

# On-device override: enabled by an opt-in sentinel file in the HA config data
# dir (NOT in the source tree, so it never travels with an rsync deploy). The
# integration reads the sentinel once at setup — see refresh_force_unsupported
# _override — so it can be toggled on a running appliance without editing code:
#   touch /config/eybond_local/force_unsupported.flag   (then restart HA)
FORCE_UNSUPPORTED_SENTINEL_NAME = "force_unsupported.flag"
_force_unsupported_override = False


def refresh_force_unsupported_override(config_data_root) -> None:
    """Re-read the on-device force-unsupported sentinel. Call from an executor."""

    global _force_unsupported_override
    if config_data_root is None:
        _force_unsupported_override = False
        return
    try:
        sentinel = Path(config_data_root) / FORCE_UNSUPPORTED_SENTINEL_NAME
        _force_unsupported_override = sentinel.exists()
    except OSError:
        _force_unsupported_override = False


def force_unsupported_models() -> bool:
    """Return whether detection must treat every device as an unsupported model."""

    return FORCE_UNSUPPORTED_MODELS or _force_unsupported_override

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
class CatalogEvidenceField:
    """One normalized evidence value emitted by a source probe action."""

    key: str
    source_key: str
    register: int = 0
    words: int = 1
    decoder: str = ""


@dataclass(frozen=True, slots=True)
class IdentityProbeSpec:
    """Registers to read and fields to extract for one transport family."""

    read_blocks: tuple[tuple[int, int], ...]
    fields: dict[str, IdentityProbeField]


@dataclass(frozen=True, slots=True)
class CatalogProbeAction:
    """One read-only source-catalog protocol action."""

    key: str
    kind: str
    cost: int
    optional: bool
    timeout: float = 0.0
    retries: int = 0
    register: int | None = None
    count: int | None = None
    # Modbus read function code: 3 = holding registers, 4 = input registers.
    function: int = 3
    command: str = ""
    parser_key: str = ""
    fields: tuple[CatalogEvidenceField, ...] = ()


@dataclass(frozen=True, slots=True)
class CatalogProtocol:
    """One canonical protocol descriptor from the source catalog."""

    key: str
    transport_key: str
    probe_actions: tuple[CatalogProbeAction, ...]
    probe_targets: tuple["CatalogProbeTarget", ...] = ()
    probe_timeout: float = 0.0
    signature_timeout: float = 0.0
    # Observed inverter-UART baud rates for this protocol family. Hints, not
    # identity: they feed the esp-bridge link sweep and diagnostics only.
    link_baud_hints: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class CatalogProbeTarget:
    """One catalog-owned EyeBond route used for protocol probing."""

    devcode: int
    collector_addr: int
    device_addr: int


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
class RuntimeValidationRule:
    """One catalog-owned runtime sanity check for a matched binding."""

    key: str
    equals: object | None = None
    one_of: tuple[object, ...] = ()
    min_value: int | float | None = None
    max_value: int | float | None = None
    known_enum: bool = False


@dataclass(frozen=True, slots=True)
class RuntimeOptionalRegister:
    """One optional diagnostic register to read after a model match."""

    key: str
    register: int
    word_count: int = 1
    signed: bool = False
    combine: str = "u16"
    divisor: int | None = None


@dataclass(frozen=True, slots=True)
class RuntimeOptionalAsciiRange:
    """One optional ASCII diagnostic register range to read after a model match."""

    key: str
    register: int
    word_count: int


@dataclass(frozen=True, slots=True)
class RuntimeProbePolicy:
    """Model-owned runtime probe validation and supplemental diagnostics."""

    validation: tuple[RuntimeValidationRule, ...] = ()
    optional_registers: tuple[RuntimeOptionalRegister, ...] = ()
    optional_ascii: tuple[RuntimeOptionalAsciiRange, ...] = ()


@dataclass(frozen=True, slots=True)
class SupportCapturePolicy:
    """Surface-owned supplemental support capture plan."""

    ranges: tuple[tuple[int, int], ...] = ()
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CatalogSurface:
    """One reusable runtime surface declared by the source catalog."""

    key: str
    protocol_key: str
    binding: CatalogBinding
    tier: str
    read_only: bool
    default_for_driver: bool = False
    support_capture: SupportCapturePolicy = SupportCapturePolicy()


@dataclass(frozen=True, slots=True)
class DeviceCatalogEntry:
    """One identified device model in the catalog."""

    entry_key: str
    surface_key: str
    fingerprint: DeviceFingerprint
    structural: tuple[str, ...]
    model_name: str
    aliases: tuple[str, ...]
    tier: str
    binding: CatalogBinding
    runtime_probe: RuntimeProbePolicy = RuntimeProbePolicy()
    devcodes: tuple[int, ...] = ()
    provenance_sources: tuple[str, ...] = ()
    provenance_confidence: str = ""
    anchors: tuple[dict[str, object], ...] = ()
    priority: int = 100
    family_fallback: bool = False


@dataclass(frozen=True, slots=True)
class FamilyDefault:
    """Reads-only fallback for a recognized layout with an unknown model."""

    surface_key: str
    when_layout_codes: tuple[int, ...]
    model_name: str
    tier: str
    binding: CatalogBinding
    runtime_probe: RuntimeProbePolicy = RuntimeProbePolicy()
    note: str = ""


@dataclass(frozen=True, slots=True)
class DeviceCatalog:
    """Parsed offline device identification catalog."""

    schema_version: int
    catalog_version: str
    protocols: dict[str, CatalogProtocol]
    transports: dict[str, IdentityProbeSpec]
    layouts: tuple[LayoutFamily, ...]
    surfaces: dict[str, CatalogSurface]
    devices: tuple[DeviceCatalogEntry, ...]
    family_defaults: tuple[FamilyDefault, ...]


@dataclass(frozen=True, slots=True)
class DeviceCatalogMatch:
    """Result of matching one identity probe against the catalog."""

    kind: str
    tier: str = ""
    entry: DeviceCatalogEntry | None = None
    entries: tuple[DeviceCatalogEntry, ...] = ()
    layout: LayoutFamily | None = None
    family_default: FamilyDefault | None = None
    confidence_signals: tuple[str, ...] = ()


@lru_cache(maxsize=None)
def load_device_catalog() -> DeviceCatalog:
    """Load and parse the offline device identification catalog."""

    raw = json.loads(DEVICE_CATALOG_PATH.read_text(encoding="utf-8"))
    protocol_items = tuple(
        item for item in raw.get("protocols", []) if isinstance(item, dict)
    )
    protocols = {
        protocol.key: protocol
        for protocol in (_parse_protocol(item) for item in protocol_items)
    }
    transports: dict[str, IdentityProbeSpec] = {}
    for protocol in protocols.values():
        spec = _identity_probe_spec(protocol)
        current = transports.get(protocol.transport_key)
        transports[protocol.transport_key] = (
            spec if current is None else _merge_identity_probe_specs(current, spec)
        )
    layouts = tuple(
        _parse_layout(layout, transport_key=protocol.transport_key)
        for item, protocol in zip(protocol_items, protocols.values(), strict=True)
        for layout in item.get("layouts", [])
        if isinstance(layout, dict)
    )
    support_capture_policies = {
        str(key).strip(): _parse_support_capture(value)
        for key, value in (raw.get("support_capture_policies") or {}).items()
        if isinstance(value, dict)
    }
    surfaces = {
        surface.key: surface
        for surface in (
            _parse_surface(item, support_capture_policies)
            for item in raw.get("surfaces", [])
            if isinstance(item, dict)
        )
    }
    devices = tuple(
        _parse_device(item, surfaces)
        for item in raw.get("devices", [])
        if isinstance(item, dict)
    )
    family_defaults = tuple(
        _parse_family_default(item, surfaces)
        for item in raw.get("family_defaults", [])
        if isinstance(item, dict)
    )
    catalog = DeviceCatalog(
        schema_version=int(raw.get("schema_version", 0)),
        catalog_version=str(raw.get("catalog_version", "")),
        protocols=protocols,
        transports=transports,
        layouts=layouts,
        surfaces=surfaces,
        devices=devices,
        family_defaults=family_defaults,
    )
    _validate_device_catalog(catalog)
    return catalog


def clear_device_catalog_cache() -> None:
    """Clear the cached device identification catalog."""

    load_device_catalog.cache_clear()
    try:
        from .detection_descriptor_loader import (
            clear_detection_descriptor_catalog_cache,
        )

        clear_detection_descriptor_catalog_cache()
    except ImportError:
        pass


def serial_ascii_plausible(serial_ascii: str) -> bool:
    """Check that a decoded serial block looks like a real ASCII serial.

    Anonymized captures scramble the serial words, so this check only ever ADDS
    a confidence signal; an implausible serial must never reject a match.
    """

    cleaned = "".join(
        char for char in str(serial_ascii or "") if char.isprintable() and char.isalnum()
    )
    return len(cleaned) >= _SERIAL_PLAUSIBLE_MIN_CHARS


def resolve_runtime_probe_policy(
    *,
    driver_key: str,
    variant_key: str,
    profile_name: str = "",
    register_schema_name: str = "",
    catalog: DeviceCatalog | None = None,
) -> RuntimeProbePolicy:
    """Resolve the catalog-owned runtime probe policy for one binding."""

    resolved = catalog if catalog is not None else load_device_catalog()
    binding = CatalogBinding(
        driver_key=str(driver_key or "").strip(),
        variant_key=str(variant_key or "").strip() or "default",
        profile_name=str(profile_name or "").strip(),
        register_schema_name=str(register_schema_name or "").strip(),
    )

    for entry in resolved.devices:
        if _binding_matches(entry.binding, binding):
            return entry.runtime_probe
    for default in resolved.family_defaults:
        if _binding_matches(default.binding, binding):
            return default.runtime_probe
    return RuntimeProbePolicy()


def resolve_catalog_surface_binding(
    driver_key: str,
    *,
    variant_key: str = "default",
    catalog: DeviceCatalog | None = None,
) -> CatalogBinding | None:
    """Resolve a source-catalog surface binding by driver and variant."""

    resolved = catalog if catalog is not None else load_device_catalog()
    normalized_driver = str(driver_key or "").strip()
    normalized_variant = str(variant_key or "default").strip() or "default"
    candidates = tuple(
        surface
        for surface in resolved.surfaces.values()
        if surface.binding.driver_key == normalized_driver
        and surface.binding.variant_key == normalized_variant
    )
    if normalized_variant == "default":
        default = next(
            (surface for surface in candidates if surface.default_for_driver),
            None,
        )
        return default.binding if default is not None else None
    if len(candidates) == 1:
        return candidates[0].binding
    return None


def resolve_support_capture_policy(
    *,
    driver_key: str,
    variant_key: str = "",
    profile_name: str = "",
    register_schema_name: str = "",
    catalog: DeviceCatalog | None = None,
) -> SupportCapturePolicy:
    """Resolve surface-owned support capture metadata for one binding."""

    resolved = catalog if catalog is not None else load_device_catalog()
    requested = CatalogBinding(
        driver_key=str(driver_key or "").strip(),
        variant_key=str(variant_key or "").strip() or "default",
        profile_name=str(profile_name or "").strip(),
        register_schema_name=str(register_schema_name or "").strip(),
    )
    for surface in resolved.surfaces.values():
        if _binding_matches(surface.binding, requested):
            return surface.support_capture
    schema_matches = tuple(
        surface
        for surface in resolved.surfaces.values()
        if surface.binding.driver_key == requested.driver_key
        and surface.binding.register_schema_name == requested.register_schema_name
    )
    if schema_matches and len(
        {surface.support_capture for surface in schema_matches}
    ) == 1:
        return schema_matches[0].support_capture
    return SupportCapturePolicy()


def _binding_matches(candidate: CatalogBinding, requested: CatalogBinding) -> bool:
    from .profile_loader import canonical_driver_profile_name

    if candidate.driver_key != requested.driver_key:
        return False
    if candidate.variant_key != requested.variant_key:
        return False
    if canonical_driver_profile_name(
        candidate.profile_name
    ) != canonical_driver_profile_name(requested.profile_name):
        return False
    if candidate.register_schema_name != requested.register_schema_name:
        return False
    return True


def _parse_protocol(raw: dict[str, object]) -> CatalogProtocol:
    return CatalogProtocol(
        key=str(raw["key"]).strip(),
        transport_key=str(raw["transport_key"]).strip(),
        probe_actions=tuple(
            _parse_probe_action(item)
            for item in raw.get("probe_actions", [])
            if isinstance(item, dict)
        ),
        probe_targets=tuple(
            CatalogProbeTarget(
                devcode=int(item["devcode"]),
                collector_addr=int(item["collector_addr"]),
                device_addr=int(item.get("device_addr", 0)),
            )
            for item in raw.get("probe_targets", [])
            if isinstance(item, dict)
        ),
        probe_timeout=float(raw.get("probe_timeout", 0.0)),
        signature_timeout=float(raw.get("signature_timeout", 0.0)),
        link_baud_hints=_parse_link_baud_hints(raw.get("link_hints")),
    )


def _parse_link_baud_hints(raw: object) -> tuple[int, ...]:
    if not isinstance(raw, dict):
        return ()
    bauds = raw.get("baud", ())
    if not isinstance(bauds, list):
        return ()
    return tuple(int(value) for value in bauds if int(value) > 0)


def _parse_probe_action(raw: dict[str, object]) -> CatalogProbeAction:
    return CatalogProbeAction(
        key=str(raw["key"]).strip(),
        kind=str(raw["kind"]).strip(),
        cost=int(raw.get("cost", 1)),
        optional=bool(raw.get("optional", False)),
        timeout=float(raw.get("timeout", 0.0)),
        retries=max(int(raw.get("retries", 0)), 0),
        register=_optional_int(raw.get("register")),
        count=_optional_int(raw.get("count")),
        function=int(raw.get("function", 3)),
        command=str(raw.get("command", "")).strip(),
        parser_key=str(raw.get("parser_key", "")).strip(),
        fields=tuple(
            CatalogEvidenceField(
                key=str(item.get("key", item["source_key"])).strip(),
                source_key=str(item["source_key"]).strip(),
                register=int(item.get("register", 0)),
                words=int(item.get("words", 1)),
                decoder=str(item.get("decoder", "")).strip(),
            )
            for item in raw.get("evidence_fields", [])
            if isinstance(item, dict)
        ),
    )


def _identity_probe_spec(protocol: CatalogProtocol) -> IdentityProbeSpec:
    modbus_actions = tuple(
        action
        for action in protocol.probe_actions
        if action.kind == "modbus_read"
        and action.register is not None
        and action.count is not None
    )
    return IdentityProbeSpec(
        read_blocks=tuple(
            (action.register, action.count)
            for action in modbus_actions
        ),
        fields={
            field.source_key: IdentityProbeField(
                register=field.register,
                words=field.words,
            )
            for action in modbus_actions
            for field in action.fields
        },
    )


def _merge_identity_probe_specs(
    left: IdentityProbeSpec,
    right: IdentityProbeSpec,
) -> IdentityProbeSpec:
    """Merge protocol identity specs that share one physical transport."""

    read_blocks = tuple(dict.fromkeys((*left.read_blocks, *right.read_blocks)))
    fields = dict(left.fields)
    fields.update(right.fields)
    return IdentityProbeSpec(read_blocks=read_blocks, fields=fields)


def _parse_layout(
    raw: dict[str, object],
    *,
    transport_key: str,
) -> LayoutFamily:
    return LayoutFamily(
        key=str(raw["key"]).strip(),
        transport=transport_key,
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


def _parse_support_capture(raw: object) -> SupportCapturePolicy:
    payload = raw if isinstance(raw, dict) else {}
    return SupportCapturePolicy(
        ranges=tuple(
            (int(item[0]), int(item[1]))
            for item in payload.get("ranges", [])
            if isinstance(item, (list, tuple)) and len(item) == 2
        ),
        notes=tuple(
            str(item).strip()
            for item in payload.get("notes", [])
            if str(item).strip()
        ),
    )


def _parse_surface(
    raw: dict[str, object],
    support_capture_policies: dict[str, SupportCapturePolicy],
) -> CatalogSurface:
    key = str(raw["key"]).strip()
    binding = _parse_binding(raw)
    tier = _validate_tier(key, str(raw.get("tier", "")).strip(), binding.profile_name)
    read_only = bool(raw.get("read_only", tier == TIER_PARTIAL))
    if read_only != (tier == TIER_PARTIAL):
        raise ValueError(f"device_catalog:{key}:surface_read_only_tier_mismatch")
    support_capture_key = str(raw.get("support_capture_policy", "")).strip()
    support_capture = support_capture_policies.get(support_capture_key)
    if support_capture is None:
        raise ValueError(
            f"device_catalog:{key}:unknown_support_capture_policy:{support_capture_key}"
        )
    return CatalogSurface(
        key=key,
        protocol_key=str(raw["protocol_key"]).strip(),
        binding=binding,
        tier=tier,
        read_only=read_only,
        default_for_driver=bool(raw.get("default_for_driver", False)),
        support_capture=support_capture,
    )


def _parse_runtime_probe(raw: object) -> RuntimeProbePolicy:
    payload = raw if isinstance(raw, dict) else {}
    return RuntimeProbePolicy(
        validation=tuple(
            _parse_runtime_validation_rule(item)
            for item in payload.get("validation", [])
            if isinstance(item, dict)
        ),
        optional_registers=tuple(
            _parse_runtime_optional_register(item)
            for item in payload.get("optional_registers", [])
            if isinstance(item, dict)
        ),
        optional_ascii=tuple(
            _parse_runtime_optional_ascii_range(item)
            for item in payload.get("optional_ascii", [])
            if isinstance(item, dict)
        ),
    )


def _parse_runtime_validation_rule(raw: dict[str, object]) -> RuntimeValidationRule:
    one_of = raw.get("one_of", ())
    if not isinstance(one_of, list):
        one_of = ()
    return RuntimeValidationRule(
        key=str(raw["key"]).strip(),
        equals=raw.get("equals"),
        one_of=tuple(one_of),
        min_value=_optional_number(raw.get("min")),
        max_value=_optional_number(raw.get("max")),
        known_enum=bool(raw.get("known_enum", False)),
    )


def _parse_runtime_optional_register(raw: dict[str, object]) -> RuntimeOptionalRegister:
    return RuntimeOptionalRegister(
        key=str(raw["key"]).strip(),
        register=int(raw["register"]),
        word_count=int(raw.get("word_count", raw.get("words", 1))),
        signed=bool(raw.get("signed", False)),
        combine=str(raw.get("combine", "u16") or "u16"),
        divisor=_optional_int(raw.get("divisor")),
    )


def _parse_runtime_optional_ascii_range(raw: dict[str, object]) -> RuntimeOptionalAsciiRange:
    return RuntimeOptionalAsciiRange(
        key=str(raw["key"]).strip(),
        register=int(raw["register"]),
        word_count=int(raw.get("word_count", raw.get("words", 1))),
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_number(value: object) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, float):
        return value
    return int(value)


_VALID_TIERS = (TIER_FULL, TIER_PARTIAL)


def _validate_tier(entry_key: str, tier: str, profile_name: str) -> str:
    """Validate a catalog tier and the tier<->controls-profile invariant.

    A typo'd tier silently dropped both the detection-summary copy and the
    partial-tier learning nudge (config_flow matches the exact strings), so
    catch it at load. The invariant: a 'full' tier carries a controls profile;
    a 'partial' tier carries none (base reads, controls locked until learning).
    """

    if tier not in _VALID_TIERS:
        raise ValueError(f"device_catalog:{entry_key}:invalid_tier:{tier!r}")
    has_profile = bool(str(profile_name or "").strip())
    if tier == TIER_FULL and not has_profile:
        raise ValueError(f"device_catalog:{entry_key}:full_tier_without_profile")
    if tier == TIER_PARTIAL and has_profile:
        raise ValueError(f"device_catalog:{entry_key}:partial_tier_with_profile")
    return tier


def _validate_device_catalog(catalog: DeviceCatalog) -> None:
    if not catalog.protocols:
        raise ValueError("device_catalog:missing_protocols")
    for protocol in catalog.protocols.values():
        if not protocol.transport_key:
            raise ValueError(f"device_catalog:{protocol.key}:missing_transport")
        if not protocol.probe_actions:
            raise ValueError(f"device_catalog:{protocol.key}:missing_probe_actions")
        if not protocol.probe_targets:
            raise ValueError(f"device_catalog:{protocol.key}:missing_probe_targets")
        for action in protocol.probe_actions:
            if action.kind == "modbus_read" and (
                action.register is None
                or action.count is None
                or action.count <= 0
            ):
                raise ValueError(
                    f"device_catalog:{protocol.key}:invalid_modbus_action:{action.key}"
                )
    default_drivers: set[str] = set()
    for surface in catalog.surfaces.values():
        if surface.protocol_key not in catalog.protocols:
            raise ValueError(
                f"device_catalog:{surface.key}:unknown_protocol:{surface.protocol_key}"
            )
        if surface.default_for_driver:
            driver_key = surface.binding.driver_key
            if driver_key in default_drivers:
                raise ValueError(
                    f"device_catalog:duplicate_default_surface:{driver_key}"
                )
            default_drivers.add(driver_key)
    fingerprints: dict[tuple[int, int], str] = {}
    for entry in catalog.devices:
        if entry.anchors:
            continue
        key = (entry.fingerprint.layout_code, entry.fingerprint.model_code)
        existing = fingerprints.get(key)
        if existing is not None:
            raise ValueError(
                "device_catalog:ambiguous_fingerprint:"
                f"{key[0]}:{key[1]}:{existing}:{entry.entry_key}"
            )
        fingerprints[key] = entry.entry_key
    claimed_layout_codes: set[int] = set()
    for default in catalog.family_defaults:
        overlap = claimed_layout_codes.intersection(default.when_layout_codes)
        if overlap:
            raise ValueError(
                "device_catalog:overlapping_family_defaults:"
                + ",".join(str(value) for value in sorted(overlap))
            )
        claimed_layout_codes.update(default.when_layout_codes)


def _surface_for_reference(
    raw: dict[str, object],
    surfaces: dict[str, CatalogSurface],
) -> CatalogSurface:
    surface_key = str(raw.get("surface_key", "")).strip()
    surface = surfaces.get(surface_key)
    if surface is None:
        raise ValueError(f"device_catalog:unknown_surface:{surface_key}")
    return surface


def _parse_device(
    raw: dict[str, object],
    surfaces: dict[str, CatalogSurface],
) -> DeviceCatalogEntry:
    provenance = raw.get("provenance")
    provenance = provenance if isinstance(provenance, dict) else {}
    cloud_hints = raw.get("cloud_hints")
    cloud_hints = cloud_hints if isinstance(cloud_hints, dict) else {}
    entry_key = str(raw["entry_key"]).strip()
    surface = _surface_for_reference(raw, surfaces)
    fingerprint_raw = raw.get("fingerprint")
    fingerprint = (
        _parse_fingerprint(fingerprint_raw)
        if isinstance(fingerprint_raw, dict)
        else DeviceFingerprint(layout_code=0, model_code=0)
    )
    return DeviceCatalogEntry(
        entry_key=entry_key,
        surface_key=surface.key,
        fingerprint=fingerprint,
        structural=tuple(str(check) for check in raw.get("structural", [])),
        model_name=str(raw.get("model_name", "")).strip(),
        aliases=tuple(
            dict.fromkeys(
                (
                    str(raw.get("model_name", "")).strip(),
                    *(
                        str(alias).strip()
                        for alias in raw.get("aliases", [])
                        if str(alias).strip()
                    ),
                )
            )
        ),
        tier=surface.tier,
        binding=surface.binding,
        runtime_probe=_parse_runtime_probe(raw.get("runtime_probe")),
        devcodes=tuple(int(code) for code in cloud_hints.get("devcodes", [])),
        provenance_sources=tuple(str(item) for item in provenance.get("sources", [])),
        provenance_confidence=str(provenance.get("confidence", "")).strip(),
        anchors=tuple(
            dict(item)
            for item in raw.get("anchors", [])
            if isinstance(item, dict)
        ),
        priority=int(raw.get("priority", 100)),
        family_fallback=bool(raw.get("family_fallback", False)),
    )


def _parse_family_default(
    raw: dict[str, object],
    surfaces: dict[str, CatalogSurface],
) -> FamilyDefault:
    surface = _surface_for_reference(raw, surfaces)
    layout_codes = tuple(int(code) for code in raw.get("when_layout_codes", []))
    return FamilyDefault(
        surface_key=surface.key,
        when_layout_codes=layout_codes,
        model_name=str(raw.get("model_name", "")).strip(),
        tier=surface.tier,
        binding=surface.binding,
        runtime_probe=_parse_runtime_probe(raw.get("runtime_probe")),
        note=str(raw.get("note", "")).strip(),
    )
