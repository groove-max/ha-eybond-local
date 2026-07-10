"""Build runtime detection descriptors from the universal inverter catalog."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from .device_catalog_loader import (
    DeviceCatalog,
    DeviceCatalogEntry,
    FamilyDefault,
    load_device_catalog,
)


_ANCHOR_COSTS: dict[str, int] = {
    "fingerprint.layout_code": 1,
    "fingerprint.model_code": 1,
    "fingerprint.rated_power": 1,
    "structural.serial_ascii_plausible": 1,
}
_MANDATORY_PROTOCOL_ANCHORS: tuple[str, ...] = (
    "fingerprint.layout_code",
    "fingerprint.model_code",
)


@dataclass(frozen=True, slots=True)
class DetectionReadBlockDescriptor:
    """One low-level read block usable by the compiled probe planner."""

    start: int
    count: int
    cost: int = 1


@dataclass(frozen=True, slots=True)
class DetectionFieldDescriptor:
    """One field extracted from a protocol-level identity probe."""

    key: str
    register: int
    words: int = 1
    source_key: str = ""
    decoder: str = ""


@dataclass(frozen=True, slots=True)
class DetectionProbeActionDescriptor:
    """One canonical read-only protocol action."""

    key: str
    kind: str
    cost: int
    optional: bool
    timeout: float
    retries: int
    fields: tuple[DetectionFieldDescriptor, ...]
    register: int | None = None
    count: int | None = None
    function: int = 3
    command: str = ""
    parser_key: str = ""


@dataclass(frozen=True, slots=True)
class DetectionProtocolDescriptor:
    """Self-contained protocol/family probing metadata."""

    key: str
    transport_key: str
    read_blocks: tuple[DetectionReadBlockDescriptor, ...]
    fields: tuple[DetectionFieldDescriptor, ...]
    probe_actions: tuple[DetectionProbeActionDescriptor, ...] = ()
    layout_keys: tuple[str, ...] = ()
    layout_codes: tuple[int, ...] = ()
    probe_targets: tuple[tuple[int, int, int], ...] = ()
    probe_timeout: float = 0.0
    signature_timeout: float = 0.0


@dataclass(frozen=True, slots=True)
class DetectionAnchorCondition:
    """One identity condition owned by a model/fallback descriptor."""

    key: str
    source: str
    required: bool = True
    equals: object | None = None
    one_of: tuple[object, ...] = ()
    min_value: int | float | None = None
    max_value: int | float | None = None
    known_enum: bool = False
    contains_any: tuple[str, ...] = ()
    cost: int = 1


@dataclass(frozen=True, slots=True)
class DetectionBindingDescriptor:
    """Runtime surface selected after a descriptor match."""

    driver_key: str
    variant_key: str
    profile_name: str = ""
    register_schema_name: str = ""
    surface_key: str = ""
    default_for_driver: bool = False
    support_capture_ranges: tuple[tuple[int, int], ...] = ()
    support_capture_notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DetectionOptionalRegisterDescriptor:
    """One model-owned optional runtime diagnostic register."""

    key: str
    register: int
    word_count: int = 1


@dataclass(frozen=True, slots=True)
class DetectionOptionalAsciiRangeDescriptor:
    """One model-owned optional runtime diagnostic ASCII range."""

    key: str
    register: int
    word_count: int


@dataclass(frozen=True, slots=True)
class DetectionDeviceDescriptor:
    """One model/fallback descriptor prepared for heuristic detection."""

    key: str
    protocol_family: str
    model_name: str
    tier: str
    binding: DetectionBindingDescriptor
    anchors: tuple[DetectionAnchorCondition, ...]
    aliases: tuple[str, ...] = ()
    optional_registers: tuple[DetectionOptionalRegisterDescriptor, ...] = ()
    optional_ascii: tuple[DetectionOptionalAsciiRangeDescriptor, ...] = ()
    provenance_sources: tuple[str, ...] = ()
    provenance_confidence: str = ""
    source_catalog: str = ""
    read_only: bool = False
    family_fallback: bool = False
    priority: int = 100

    @property
    def required_anchor_keys(self) -> tuple[str, ...]:
        """Return required anchor keys in declaration order."""

        return tuple(anchor.key for anchor in self.anchors if anchor.required)


@dataclass(frozen=True, slots=True)
class DetectionDescriptorCatalog:
    """Unified descriptor catalog used by runtime heuristic planning."""

    protocols: dict[str, DetectionProtocolDescriptor]
    devices: tuple[DetectionDeviceDescriptor, ...]

    def descriptors_for_protocol(self, protocol_family: str) -> tuple[DetectionDeviceDescriptor, ...]:
        """Return model/fallback descriptors for one protocol family."""

        normalized = str(protocol_family or "").strip()
        return tuple(device for device in self.devices if device.protocol_family == normalized)

    def descriptor_for_key(self, key: str) -> DetectionDeviceDescriptor | None:
        """Resolve one descriptor by key."""

        normalized = str(key or "").strip()
        for device in self.devices:
            if device.key == normalized:
                return device
        return None


@lru_cache(maxsize=None)
def load_detection_descriptor_catalog() -> DetectionDescriptorCatalog:
    """Load the normalized descriptor catalog from existing declarative sources."""

    device_catalog = load_device_catalog()
    protocols = _build_protocol_descriptors(device_catalog)
    devices = tuple(_build_device_descriptors(device_catalog))
    catalog = DetectionDescriptorCatalog(protocols=protocols, devices=devices)
    validate_detection_descriptor_catalog(catalog)
    return catalog


def clear_detection_descriptor_catalog_cache() -> None:
    """Clear cached descriptor catalog metadata."""

    load_detection_descriptor_catalog.cache_clear()
    try:
        from .compiled_detection_catalog import (
            clear_compiled_detection_catalog_cache,
        )

        clear_compiled_detection_catalog_cache()
    except ImportError:
        pass


def validate_detection_descriptor_catalog(catalog: DetectionDescriptorCatalog) -> None:
    """Validate one descriptor catalog instance."""

    _validate_detection_descriptor_catalog(catalog)


def detection_anchor_cost(anchor_key: str) -> int:
    """Return a stable relative probe cost for one anchor key."""

    normalized = str(anchor_key or "").strip()
    if normalized in _ANCHOR_COSTS:
        return _ANCHOR_COSTS[normalized]
    if normalized.startswith("fingerprint."):
        return 1
    return 5


def mandatory_detection_anchor_keys(
    protocol_family: str = "modbus_smg",
) -> tuple[str, ...]:
    """Return protocol proof anchors that should be read before variant scoring."""

    if str(protocol_family).startswith("pi"):
        return ("protocol.protocol_id",)
    return _MANDATORY_PROTOCOL_ANCHORS


def _build_protocol_descriptors(
    device_catalog: DeviceCatalog,
) -> dict[str, DetectionProtocolDescriptor]:
    by_protocol: dict[str, DetectionProtocolDescriptor] = {}
    for protocol_family, source_protocol in device_catalog.protocols.items():
        probe_spec = device_catalog.transports[source_protocol.transport_key]
        layouts = tuple(
            layout
            for layout in device_catalog.layouts
            if layout.transport == source_protocol.transport_key
        )
        by_protocol[protocol_family] = DetectionProtocolDescriptor(
            key=protocol_family,
            transport_key=source_protocol.transport_key,
            read_blocks=tuple(
                DetectionReadBlockDescriptor(start=start, count=count)
                for start, count in probe_spec.read_blocks
            ),
            fields=tuple(
                DetectionFieldDescriptor(
                    key=field.key,
                    register=field.register,
                    words=field.words,
                    source_key=field.source_key,
                    decoder=field.decoder,
                )
                for action in source_protocol.probe_actions
                for field in action.fields
            ),
            probe_actions=tuple(
                DetectionProbeActionDescriptor(
                    key=action.key,
                    kind=action.kind,
                    cost=action.cost,
                    optional=action.optional,
                    timeout=action.timeout,
                    retries=action.retries,
                    register=action.register,
                    count=action.count,
                    function=action.function,
                    command=action.command,
                    parser_key=action.parser_key,
                    fields=tuple(
                        DetectionFieldDescriptor(
                            key=field.key,
                            register=field.register,
                            words=field.words,
                            source_key=field.source_key,
                            decoder=field.decoder,
                        )
                        for field in action.fields
                    ),
                )
                for action in source_protocol.probe_actions
            ),
            layout_keys=tuple(layout.key for layout in layouts),
            layout_codes=tuple(
                code for layout in layouts for code in layout.layout_codes
            ),
            probe_targets=tuple(
                (target.devcode, target.collector_addr, target.device_addr)
                for target in source_protocol.probe_targets
            ),
            probe_timeout=source_protocol.probe_timeout,
            signature_timeout=source_protocol.signature_timeout,
        )
    return by_protocol


def _build_device_descriptors(
    device_catalog: DeviceCatalog,
) -> list[DetectionDeviceDescriptor]:
    descriptors = [
        _descriptor_from_device_entry(entry)
        for entry in device_catalog.devices
    ]
    descriptors.extend(
        _descriptor_from_family_default(default)
        for default in device_catalog.family_defaults
    )
    return descriptors


def _descriptor_from_device_entry(entry: DeviceCatalogEntry) -> DetectionDeviceDescriptor:
    catalog_surface = load_device_catalog().surfaces[entry.surface_key]
    protocol_family = _protocol_family_for_surface(entry.surface_key)
    binding = DetectionBindingDescriptor(
        driver_key=entry.binding.driver_key,
        variant_key=entry.binding.variant_key,
        profile_name=entry.binding.profile_name,
        register_schema_name=entry.binding.register_schema_name,
        surface_key=entry.surface_key,
        default_for_driver=catalog_surface.default_for_driver,
        support_capture_ranges=catalog_surface.support_capture.ranges,
        support_capture_notes=catalog_surface.support_capture.notes,
    )
    anchors: list[DetectionAnchorCondition] = []
    if entry.anchors:
        anchors.extend(_parse_catalog_anchors(entry.anchors))
    else:
        anchors.extend(
            (
                DetectionAnchorCondition(
                    key="fingerprint.layout_code",
                    source="inverter_catalog.fingerprint",
                    equals=entry.fingerprint.layout_code,
                    cost=detection_anchor_cost("fingerprint.layout_code"),
                ),
                DetectionAnchorCondition(
                    key="fingerprint.model_code",
                    source="inverter_catalog.fingerprint",
                    equals=entry.fingerprint.model_code,
                    cost=detection_anchor_cost("fingerprint.model_code"),
                ),
            )
        )
    if not entry.anchors and entry.fingerprint.rated_power_one_of:
        anchors.append(
            DetectionAnchorCondition(
                key="fingerprint.rated_power",
                source="device_catalog.fingerprint",
                required=False,
                one_of=entry.fingerprint.rated_power_one_of,
                cost=detection_anchor_cost("fingerprint.rated_power"),
            )
        )
    for structural_check in entry.structural:
        anchors.append(
            DetectionAnchorCondition(
                key=f"structural.{structural_check}",
                source="device_catalog.structural",
                required=False,
                equals=True,
                cost=detection_anchor_cost(f"structural.{structural_check}"),
            )
        )
    anchors.extend(_runtime_validation_anchors(entry.runtime_probe.validation))

    return DetectionDeviceDescriptor(
        key=entry.entry_key,
        protocol_family=protocol_family,
        model_name=entry.model_name,
        aliases=entry.aliases,
        tier=entry.tier,
        binding=binding,
        anchors=tuple(anchors),
        optional_registers=_optional_register_descriptors(
            entry.runtime_probe.optional_registers
        ),
        optional_ascii=_optional_ascii_descriptors(entry.runtime_probe.optional_ascii),
        provenance_sources=entry.provenance_sources,
        provenance_confidence=entry.provenance_confidence,
        source_catalog="device_catalog",
        read_only=not bool(entry.binding.profile_name),
        family_fallback=entry.family_fallback,
        priority=entry.priority,
    )


def _descriptor_from_family_default(default: FamilyDefault) -> DetectionDeviceDescriptor:
    catalog_surface = load_device_catalog().surfaces[default.surface_key]
    protocol_family = _protocol_family_for_surface(default.surface_key)
    binding = DetectionBindingDescriptor(
        driver_key=default.binding.driver_key,
        variant_key=default.binding.variant_key,
        profile_name=default.binding.profile_name,
        register_schema_name=default.binding.register_schema_name,
        surface_key=default.surface_key,
        default_for_driver=catalog_surface.default_for_driver,
        support_capture_ranges=catalog_surface.support_capture.ranges,
        support_capture_notes=catalog_surface.support_capture.notes,
    )
    return DetectionDeviceDescriptor(
        key=f"{protocol_family}.{default.binding.variant_key}",
        protocol_family=protocol_family,
        model_name=default.model_name,
        aliases=(default.model_name,),
        tier=default.tier,
        binding=binding,
        anchors=(
            DetectionAnchorCondition(
                key="fingerprint.layout_code",
                source="device_catalog.family_default",
                one_of=default.when_layout_codes,
                cost=detection_anchor_cost("fingerprint.layout_code"),
            ),
            *_runtime_validation_anchors(default.runtime_probe.validation),
        ),
        optional_registers=_optional_register_descriptors(
            default.runtime_probe.optional_registers
        ),
        optional_ascii=_optional_ascii_descriptors(default.runtime_probe.optional_ascii),
        provenance_sources=(default.note,) if default.note else (),
        source_catalog="device_catalog.family_defaults",
        read_only=True,
        family_fallback=True,
        priority=0,
    )


def _parse_catalog_anchors(
    anchors: tuple[dict[str, object], ...],
) -> tuple[DetectionAnchorCondition, ...]:
    return tuple(
        DetectionAnchorCondition(
            key=str(raw["key"]).strip(),
            source="inverter_catalog.anchors",
            required=bool(raw.get("required", True)),
            equals=raw.get("equals"),
            one_of=tuple(raw.get("one_of", ())),
            min_value=raw.get("min"),
            max_value=raw.get("max"),
            known_enum=bool(raw.get("known_enum", False)),
            contains_any=tuple(
                str(value).lower()
                for value in raw.get("contains_any", ())
            ),
            cost=int(raw.get("cost", detection_anchor_cost(str(raw["key"])))),
        )
        for raw in anchors
    )


def _runtime_validation_anchors(rules) -> tuple[DetectionAnchorCondition, ...]:
    return tuple(
        DetectionAnchorCondition(
            key=f"runtime.{rule.key}",
            source="device_catalog.runtime_probe.validation",
            required=False,
            equals=rule.equals,
            one_of=rule.one_of,
            min_value=rule.min_value,
            max_value=rule.max_value,
            known_enum=rule.known_enum,
            cost=detection_anchor_cost(f"runtime.{rule.key}"),
        )
        for rule in rules
    )


def _optional_register_descriptors(items) -> tuple[DetectionOptionalRegisterDescriptor, ...]:
    return tuple(
        DetectionOptionalRegisterDescriptor(
            key=item.key,
            register=item.register,
            word_count=item.word_count,
        )
        for item in items
    )


def _optional_ascii_descriptors(items) -> tuple[DetectionOptionalAsciiRangeDescriptor, ...]:
    return tuple(
        DetectionOptionalAsciiRangeDescriptor(
            key=item.key,
            register=item.register,
            word_count=item.word_count,
        )
        for item in items
    )


def _protocol_family_for_surface(surface_key: str) -> str:
    catalog = load_device_catalog()
    protocol_key = catalog.surfaces[surface_key].protocol_key
    if protocol_key not in catalog.protocols:
        raise ValueError(
            f"detection_descriptor_catalog:surface_protocol_unresolved:{surface_key}"
        )
    return protocol_key


def _validate_detection_descriptor_catalog(catalog: DetectionDescriptorCatalog) -> None:
    if not catalog.protocols:
        raise ValueError("detection_descriptor_catalog:missing_protocols")
    if not catalog.devices:
        raise ValueError("detection_descriptor_catalog:missing_devices")

    seen_protocol_keys: set[str] = set()
    for key, protocol in catalog.protocols.items():
        if not key:
            raise ValueError("detection_descriptor_catalog:invalid_protocol_key")
        if protocol.key != key:
            raise ValueError(
                f"detection_descriptor_catalog:protocol_key_mismatch:{protocol.key}:{key}"
            )
        if key in seen_protocol_keys:
            raise ValueError(f"detection_descriptor_catalog:duplicate_protocol:{key}")
        seen_protocol_keys.add(key)
        if not protocol.transport_key:
            raise ValueError(f"detection_descriptor_catalog:missing_protocol_transport:{key}")
        if not protocol.probe_actions:
            raise ValueError(f"detection_descriptor_catalog:missing_protocol_actions:{key}")
        if not protocol.fields:
            raise ValueError(f"detection_descriptor_catalog:missing_protocol_fields:{key}")

    seen_device_keys: set[str] = set()
    for descriptor in catalog.devices:
        if not descriptor.key:
            raise ValueError("detection_descriptor_catalog:invalid_device_key")
        if descriptor.key in seen_device_keys:
            raise ValueError(f"detection_descriptor_catalog:duplicate_device:{descriptor.key}")
        seen_device_keys.add(descriptor.key)
        if descriptor.protocol_family not in catalog.protocols:
            raise ValueError(
                "detection_descriptor_catalog:unknown_device_protocol:"
                f"{descriptor.key}:{descriptor.protocol_family}"
            )
        if not descriptor.binding.driver_key:
            raise ValueError(
                f"detection_descriptor_catalog:missing_binding_driver:{descriptor.key}"
            )
        if not descriptor.binding.variant_key:
            raise ValueError(
                f"detection_descriptor_catalog:missing_binding_variant:{descriptor.key}"
            )
        if not descriptor.anchors:
            raise ValueError(f"detection_descriptor_catalog:missing_anchors:{descriptor.key}")
        if descriptor.tier == "full" and not descriptor.binding.profile_name:
            raise ValueError(
                f"detection_descriptor_catalog:full_without_profile:{descriptor.key}"
            )
        if descriptor.tier == "partial" and descriptor.binding.profile_name:
            raise ValueError(
                f"detection_descriptor_catalog:partial_with_profile:{descriptor.key}"
            )
        _validate_anchor_conditions(descriptor)


def _validate_anchor_conditions(descriptor: DetectionDeviceDescriptor) -> None:
    seen_anchor_keys: set[str] = set()
    for anchor in descriptor.anchors:
        if not anchor.key:
            raise ValueError(
                f"detection_descriptor_catalog:invalid_anchor:{descriptor.key}"
            )
        if anchor.key in seen_anchor_keys:
            raise ValueError(
                f"detection_descriptor_catalog:duplicate_anchor:{descriptor.key}:{anchor.key}"
            )
        seen_anchor_keys.add(anchor.key)
        if not anchor.source:
            raise ValueError(
                f"detection_descriptor_catalog:missing_anchor_source:{descriptor.key}:{anchor.key}"
            )
        if anchor.cost <= 0:
            raise ValueError(
                f"detection_descriptor_catalog:invalid_anchor_cost:{descriptor.key}:{anchor.key}"
            )
        _validate_anchor_has_condition(descriptor.key, anchor)


def _validate_anchor_has_condition(
    descriptor_key: str,
    anchor: DetectionAnchorCondition,
) -> None:
    if not anchor.required:
        return
    has_condition = (
        anchor.equals is not None
        or bool(anchor.one_of)
        or anchor.min_value is not None
            or anchor.max_value is not None
            or anchor.known_enum
            or bool(anchor.contains_any)
        )
    if not has_condition:
        raise ValueError(
            f"detection_descriptor_catalog:required_anchor_without_condition:"
            f"{descriptor_key}:{anchor.key}"
        )


def _candidate_anchor_keys(
    descriptors: tuple[DetectionDeviceDescriptor, ...],
) -> tuple[str, ...]:
    keys: set[str] = set()
    for descriptor in descriptors:
        keys.update(anchor.key for anchor in descriptor.anchors if anchor.required)
    return tuple(sorted(keys))


def _stable_signature_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    # Integral floats must collapse to the int spelling: catalog anchors say
    # equals: 220 while parsed telemetry arrives as 220.0, and the signature
    # match is a STRING comparison. (The retired value-based engine compared
    # numerics natively, which masked this divergence.)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)

