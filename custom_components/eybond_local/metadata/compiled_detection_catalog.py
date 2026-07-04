"""Compile declarative detection metadata into indexed runtime structures."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
from typing import Mapping

from .detection_decision_tree import (
    DetectionDecisionTree,
    build_detection_decision_tree,
    evaluate_detection_decision_tree,
)
from .detection_descriptor_loader import (
    DetectionAnchorCondition,
    DetectionDescriptorCatalog,
    DetectionDeviceDescriptor,
    DetectionProtocolDescriptor,
    load_detection_descriptor_catalog,
)
from .anchor_matching import anchor_condition_matches
from .device_catalog_loader import load_device_catalog


PROBE_ACTION_MODBUS_READ = "modbus_read"
PROBE_ACTION_ASCII_COMMAND = "ascii_command"
PROBE_ACTION_SMARTESS_QUERY = "smartess_query"
PROBE_ACTION_COLLECTOR_METADATA = "collector_metadata"
SUPPORTED_PROBE_ACTION_KINDS = frozenset(
    {
        PROBE_ACTION_MODBUS_READ,
        PROBE_ACTION_ASCII_COMMAND,
        PROBE_ACTION_SMARTESS_QUERY,
        PROBE_ACTION_COLLECTOR_METADATA,
    }
)

RESOLUTION_EXACT = "exact"
RESOLUTION_COMPATIBLE_GROUP = "compatible_group"
RESOLUTION_FAMILY = "family"
RESOLUTION_UNRESOLVED = "unresolved"


@dataclass(frozen=True, slots=True)
class CompiledEvidenceField:
    """One normalized evidence value produced by a probe action."""

    key: str
    source_key: str
    register: int
    words: int = 1
    decoder: str = "u16"


@dataclass(frozen=True, slots=True)
class CompiledProbeAction:
    """One executable read-only protocol probe action."""

    key: str
    protocol_key: str
    kind: str
    cost: int
    optional: bool
    timeout: float
    retries: int
    evidence_fields: tuple[CompiledEvidenceField, ...]
    register: int | None = None
    count: int | None = None
    function: int = 3
    command: str = ""
    parser_key: str = ""


@dataclass(frozen=True, slots=True)
class CompiledProtocolDescriptor:
    """One protocol with its ordered probe actions."""

    key: str
    transport_key: str
    probe_actions: tuple[CompiledProbeAction, ...]
    layout_keys: tuple[str, ...] = ()
    layout_codes: tuple[int, ...] = ()
    probe_targets: tuple[tuple[int, int, int], ...] = ()
    probe_timeout: float = 0.0
    signature_timeout: float = 0.0


@dataclass(frozen=True, slots=True)
class CompiledSurfaceDescriptor:
    """One reusable runtime profile/schema surface."""

    key: str
    driver_key: str
    variant_key: str
    profile_name: str
    register_schema_name: str
    support_tier: str
    read_only: bool
    default_for_driver: bool = False
    support_capture_ranges: tuple[tuple[int, int], ...] = ()
    support_capture_notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CompiledDeviceDescriptor:
    """One device/family candidate linked to a compiled surface."""

    key: str
    protocol_key: str
    model_name: str
    aliases: tuple[str, ...]
    surface_key: str
    anchors: tuple[DetectionAnchorCondition, ...]
    provenance_sources: tuple[str, ...]
    provenance_confidence: str
    family_fallback: bool
    priority: int = 100
    revision: str = ""


@dataclass(frozen=True, slots=True)
class CompiledResolutionResult:
    """One explicit device/surface resolution outcome."""

    protocol_key: str
    resolution: str
    candidate_keys: tuple[str, ...]
    surface_key: str | None
    confidence: str
    missing_evidence_keys: tuple[str, ...] = ()
    collected_evidence_keys: tuple[str, ...] = ()
    failed_evidence_keys: tuple[str, ...] = ()
    unsupported_evidence_keys: tuple[str, ...] = ()
    contradicting_evidence_keys: tuple[str, ...] = ()
    decision_path: tuple[str, ...] = ()
    catalog_version: str = ""
    descriptor_revisions: tuple[str, ...] = ()
    evidence_fingerprint: str = ""

    @property
    def resolved(self) -> bool:
        """Return whether a safe runtime surface was selected."""

        return self.surface_key is not None


@dataclass(frozen=True, slots=True)
class CompiledDetectionCatalog:
    """Indexed runtime representation of the source detection catalog."""

    schema_version: int
    catalog_version: str
    protocols: dict[str, CompiledProtocolDescriptor]
    protocols_by_transport: dict[str, tuple[str, ...]]
    devices: dict[str, CompiledDeviceDescriptor]
    surfaces: dict[str, CompiledSurfaceDescriptor]
    devices_by_protocol: dict[str, tuple[str, ...]]
    devices_by_surface: dict[str, tuple[str, ...]]
    devices_by_evidence_key: dict[str, tuple[str, ...]]
    devices_by_alias: dict[str, tuple[str, ...]]
    exact_evidence_index: dict[tuple[str, str, object], tuple[str, ...]]
    decision_trees: dict[str, DetectionDecisionTree]

    def resolve_family(
        self,
        *,
        protocol_key: str,
        evidence: Mapping[str, object],
    ) -> CompiledResolutionResult:
        """Resolve the safest matching family fallback for observed evidence."""

        normalized_protocol = str(protocol_key or "").strip()
        candidates = tuple(
            descriptor
            for descriptor in self.devices.values()
            if descriptor.protocol_key == normalized_protocol
            and descriptor.family_fallback
            and not _descriptor_contradicts(descriptor, evidence)
            and _descriptor_has_required_evidence(descriptor, evidence)
        )
        return self.resolution_for_candidates(
            protocol_key=normalized_protocol,
            candidate_keys=tuple(descriptor.key for descriptor in candidates),
            evidence=evidence,
        )

    def resolution_for_candidates(
        self,
        *,
        protocol_key: str,
        candidate_keys: tuple[str, ...],
        evidence: Mapping[str, object],
        decision_path: tuple[str, ...] = (),
    ) -> CompiledResolutionResult:
        """Build a resolution from an explicit decision-tree candidate set."""

        candidates = tuple(
            self.devices[key]
            for key in candidate_keys
            if key in self.devices and self.devices[key].protocol_key == protocol_key
        )
        if not candidates:
            return self._build_resolution(
                protocol_key=protocol_key,
                resolution=RESOLUTION_UNRESOLVED,
                candidates=(),
                surface_key=None,
                confidence="none",
                evidence=evidence,
                decision_path=decision_path,
            )
        if len(candidates) == 1:
            descriptor = candidates[0]
            return self._build_resolution(
                protocol_key=protocol_key,
                resolution=RESOLUTION_FAMILY if descriptor.family_fallback else RESOLUTION_EXACT,
                candidates=candidates,
                surface_key=descriptor.surface_key,
                confidence="medium" if descriptor.family_fallback else "high",
                evidence=evidence,
                decision_path=decision_path,
            )
        surface_keys = {candidate.surface_key for candidate in candidates}
        if len(surface_keys) == 1:
            return self._build_resolution(
                protocol_key=protocol_key,
                resolution=RESOLUTION_COMPATIBLE_GROUP,
                candidates=candidates,
                surface_key=next(iter(surface_keys)),
                confidence="medium",
                evidence=evidence,
                decision_path=decision_path,
            )
        return self._build_resolution(
            protocol_key=protocol_key,
            resolution=RESOLUTION_UNRESOLVED,
            candidates=candidates,
            surface_key=None,
            confidence="none",
            evidence=evidence,
            decision_path=decision_path,
        )

    def _build_resolution(
        self,
        *,
        protocol_key: str,
        resolution: str,
        candidates: tuple[CompiledDeviceDescriptor, ...],
        surface_key: str | None,
        confidence: str,
        evidence: Mapping[str, object],
        decision_path: tuple[str, ...],
    ) -> CompiledResolutionResult:
        return CompiledResolutionResult(
            protocol_key=protocol_key,
            resolution=resolution,
            candidate_keys=tuple(sorted(candidate.key for candidate in candidates)),
            surface_key=surface_key,
            confidence=confidence,
            collected_evidence_keys=tuple(sorted(evidence)),
            contradicting_evidence_keys=tuple(
                sorted(
                    {
                        anchor.key
                        for candidate in candidates
                        for anchor in candidate.anchors
                        if anchor.key in evidence
                        and not _anchor_matches(anchor, evidence[anchor.key])
                    }
                )
            ),
            decision_path=decision_path,
            catalog_version=self.catalog_version,
            descriptor_revisions=tuple(
                f"{candidate.key}:{candidate.revision}"
                for candidate in sorted(candidates, key=lambda item: item.key)
            ),
            evidence_fingerprint=_evidence_fingerprint(evidence),
        )


@lru_cache(maxsize=None)
def load_compiled_detection_catalog() -> CompiledDetectionCatalog:
    """Compile and cache the current declarative detection catalog."""

    source = load_detection_descriptor_catalog()
    device_catalog = load_device_catalog()
    return compile_detection_catalog(
        source,
        schema_version=device_catalog.schema_version,
        catalog_version=device_catalog.catalog_version,
    )


def compile_detection_catalog(
    source: DetectionDescriptorCatalog,
    *,
    schema_version: int,
    catalog_version: str,
) -> CompiledDetectionCatalog:
    """Compile one validated descriptor catalog into runtime indexes."""

    protocols = {
        key: _compile_protocol(protocol)
        for key, protocol in source.protocols.items()
    }

    surfaces: dict[str, CompiledSurfaceDescriptor] = {}
    devices: dict[str, CompiledDeviceDescriptor] = {}
    for descriptor in source.devices:
        surface = _compile_surface(descriptor)
        existing_surface = surfaces.get(surface.key)
        if existing_surface is not None and existing_surface != surface:
            raise ValueError(
                f"compiled_detection_catalog:conflicting_surface:{surface.key}"
            )
        surfaces[surface.key] = surface
        devices[descriptor.key] = CompiledDeviceDescriptor(
            key=descriptor.key,
            protocol_key=descriptor.protocol_family,
            model_name=descriptor.model_name,
            aliases=descriptor.aliases,
            surface_key=surface.key,
            anchors=descriptor.anchors,
            provenance_sources=descriptor.provenance_sources,
            provenance_confidence=descriptor.provenance_confidence,
            family_fallback=descriptor.family_fallback,
            priority=descriptor.priority,
            revision=_descriptor_revision(descriptor),
        )

    devices_by_protocol = _group_device_keys(
        devices,
        key=lambda descriptor: descriptor.protocol_key,
    )
    devices_by_surface = _group_device_keys(
        devices,
        key=lambda descriptor: descriptor.surface_key,
    )
    devices_by_evidence_key = _build_evidence_device_index(devices)
    exact_evidence_index = _build_exact_evidence_index(devices)
    protocols_by_transport = _group_protocol_keys(protocols)
    devices_by_alias = _build_alias_index(devices)
    decision_trees = {
        protocol_key: build_detection_decision_tree(
            protocol_family=protocol_key,
            catalog=source,
        )
        for protocol_key in protocols
    }

    compiled = CompiledDetectionCatalog(
        schema_version=int(schema_version),
        catalog_version=str(catalog_version),
        protocols=protocols,
        protocols_by_transport=protocols_by_transport,
        devices=devices,
        surfaces=surfaces,
        devices_by_protocol=devices_by_protocol,
        devices_by_surface=devices_by_surface,
        devices_by_evidence_key=devices_by_evidence_key,
        devices_by_alias=devices_by_alias,
        exact_evidence_index=exact_evidence_index,
        decision_trees=decision_trees,
    )
    validate_compiled_detection_catalog(compiled)
    return compiled


def clear_compiled_detection_catalog_cache() -> None:
    """Clear the compiled runtime catalog."""

    load_compiled_detection_catalog.cache_clear()


def validate_compiled_detection_catalog(catalog: CompiledDetectionCatalog) -> None:
    """Validate compiled references and read-only probe constraints."""

    if not catalog.protocols:
        raise ValueError("compiled_detection_catalog:missing_protocols")
    if not catalog.devices:
        raise ValueError("compiled_detection_catalog:missing_devices")
    for protocol in catalog.protocols.values():
        if not protocol.probe_actions:
            raise ValueError(
                f"compiled_detection_catalog:missing_probe_actions:{protocol.key}"
            )
        for action in protocol.probe_actions:
            if action.kind not in SUPPORTED_PROBE_ACTION_KINDS:
                raise ValueError(
                    f"compiled_detection_catalog:unsupported_probe_action:{action.kind}"
                )
            if (
                action.kind == PROBE_ACTION_MODBUS_READ
                and (
                    action.register is None
                    or action.count is None
                    or action.count <= 0
                )
            ):
                raise ValueError(
                    f"compiled_detection_catalog:invalid_modbus_action:{action.key}"
                )
            if action.kind == PROBE_ACTION_ASCII_COMMAND and not action.command:
                raise ValueError(
                    f"compiled_detection_catalog:invalid_ascii_action:{action.key}"
                )
    for device in catalog.devices.values():
        if device.protocol_key not in catalog.protocols:
            raise ValueError(
                f"compiled_detection_catalog:unknown_protocol:{device.key}"
            )
        if device.surface_key not in catalog.surfaces:
            raise ValueError(
                f"compiled_detection_catalog:unknown_surface:{device.key}"
            )
    default_drivers: set[str] = set()
    for surface in catalog.surfaces.values():
        if not surface.default_for_driver:
            continue
        if surface.driver_key in default_drivers:
            raise ValueError(
                "compiled_detection_catalog:duplicate_default_surface:"
                f"{surface.driver_key}"
            )
        default_drivers.add(surface.driver_key)


def _compile_protocol(
    protocol: DetectionProtocolDescriptor,
) -> CompiledProtocolDescriptor:
    if protocol.probe_actions:
        return CompiledProtocolDescriptor(
            key=protocol.key,
            transport_key=protocol.transport_key,
            probe_actions=tuple(
                CompiledProbeAction(
                    key=action.key,
                    protocol_key=protocol.key,
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
                    evidence_fields=tuple(
                        _compile_evidence_field(field)
                        for field in action.fields
                    ),
                )
                for action in protocol.probe_actions
            ),
            layout_keys=protocol.layout_keys,
            layout_codes=protocol.layout_codes,
            probe_targets=protocol.probe_targets,
            probe_timeout=protocol.probe_timeout,
            signature_timeout=protocol.signature_timeout,
        )
    fields = tuple(protocol.fields)
    actions: list[CompiledProbeAction] = []
    for block in protocol.read_blocks:
        block_fields = tuple(
            _compile_evidence_field(field)
            for field in fields
            if block.start <= field.register < block.start + block.count
        )
        actions.append(
            CompiledProbeAction(
                key=f"{protocol.key}.modbus.{block.start}.{block.count}",
                protocol_key=protocol.key,
                kind=PROBE_ACTION_MODBUS_READ,
                cost=block.cost,
                optional=False,
                timeout=0.0,
                retries=0,
                register=block.start,
                count=block.count,
                evidence_fields=block_fields,
            )
        )
    return CompiledProtocolDescriptor(
        key=protocol.key,
        transport_key=protocol.transport_key,
        probe_actions=tuple(actions),
        layout_keys=protocol.layout_keys,
        layout_codes=protocol.layout_codes,
        probe_targets=protocol.probe_targets,
        probe_timeout=protocol.probe_timeout,
        signature_timeout=protocol.signature_timeout,
    )


def _compile_evidence_field(field) -> CompiledEvidenceField:
    evidence_key = {
        "layout_code": "fingerprint.layout_code",
        "model_code": "fingerprint.model_code",
        "rated_power": "fingerprint.rated_power",
        "serial_ascii": "identity.serial_ascii",
    }.get(field.source_key or field.key, field.key)
    return CompiledEvidenceField(
        key=evidence_key,
        source_key=field.source_key or field.key,
        register=field.register,
        words=field.words,
        decoder=field.decoder or (
            "ascii" if (field.source_key or field.key) == "serial_ascii" else "u16"
        ),
    )


def _compile_surface(
    descriptor: DetectionDeviceDescriptor,
) -> CompiledSurfaceDescriptor:
    binding = descriptor.binding
    surface_key = binding.surface_key or _surface_key(
        driver_key=binding.driver_key,
        variant_key=binding.variant_key,
        profile_name=binding.profile_name,
        register_schema_name=binding.register_schema_name,
    )
    return CompiledSurfaceDescriptor(
        key=surface_key,
        driver_key=binding.driver_key,
        variant_key=binding.variant_key,
        profile_name=binding.profile_name,
        register_schema_name=binding.register_schema_name,
        support_tier=descriptor.tier,
        read_only=descriptor.read_only,
        default_for_driver=binding.default_for_driver,
        support_capture_ranges=binding.support_capture_ranges,
        support_capture_notes=binding.support_capture_notes,
    )


def _surface_key(
    *,
    driver_key: str,
    variant_key: str,
    profile_name: str,
    register_schema_name: str,
) -> str:
    profile_token = profile_name or "none"
    schema_token = register_schema_name or "none"
    return "|".join(
        (
            str(driver_key or "").strip(),
            str(variant_key or "").strip(),
            profile_token,
            schema_token,
        )
    )


def _group_device_keys(devices, *, key) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for descriptor in devices.values():
        grouped.setdefault(key(descriptor), []).append(descriptor.key)
    return {
        group_key: tuple(sorted(device_keys))
        for group_key, device_keys in grouped.items()
    }


def _group_protocol_keys(
    protocols: Mapping[str, CompiledProtocolDescriptor],
) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for protocol in protocols.values():
        grouped.setdefault(protocol.transport_key, []).append(protocol.key)
    return {
        transport_key: tuple(sorted(protocol_keys))
        for transport_key, protocol_keys in grouped.items()
    }


def _build_alias_index(
    devices: Mapping[str, CompiledDeviceDescriptor],
) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for descriptor in devices.values():
        for alias in descriptor.aliases:
            normalized = alias.strip().casefold()
            if normalized:
                grouped.setdefault(normalized, []).append(descriptor.key)
    return {
        alias: tuple(sorted(set(device_keys)))
        for alias, device_keys in grouped.items()
    }


def _build_evidence_device_index(
    devices: Mapping[str, CompiledDeviceDescriptor],
) -> dict[str, tuple[str, ...]]:
    index: dict[str, list[str]] = {}
    for descriptor in devices.values():
        for anchor in descriptor.anchors:
            index.setdefault(anchor.key, []).append(descriptor.key)
    return {
        evidence_key: tuple(sorted(set(device_keys)))
        for evidence_key, device_keys in index.items()
    }


def _build_exact_evidence_index(
    devices: Mapping[str, CompiledDeviceDescriptor],
) -> dict[tuple[str, str, object], tuple[str, ...]]:
    index: dict[tuple[str, str, object], list[str]] = {}
    for descriptor in devices.values():
        for anchor in descriptor.anchors:
            if anchor.equals is None:
                continue
            key = (descriptor.protocol_key, anchor.key, anchor.equals)
            index.setdefault(key, []).append(descriptor.key)
    return {
        index_key: tuple(sorted(set(device_keys)))
        for index_key, device_keys in index.items()
    }


def _descriptor_revision(descriptor: DetectionDeviceDescriptor) -> str:
    payload = {
        "key": descriptor.key,
        "protocol": descriptor.protocol_family,
        "surface": descriptor.binding.surface_key,
        "aliases": descriptor.aliases,
        "priority": descriptor.priority,
        "anchors": [
            {
                "key": anchor.key,
                "required": anchor.required,
                "equals": anchor.equals,
                "one_of": anchor.one_of,
                "min": anchor.min_value,
                "max": anchor.max_value,
                "known_enum": anchor.known_enum,
                "contains_any": anchor.contains_any,
            }
            for anchor in descriptor.anchors
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def _evidence_fingerprint(evidence: Mapping[str, object]) -> str:
    encoded = json.dumps(
        dict(evidence),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _descriptor_contradicts(
    descriptor: CompiledDeviceDescriptor,
    evidence: Mapping[str, object],
) -> bool:
    for anchor in descriptor.anchors:
        if anchor.key not in evidence:
            continue
        if not _anchor_matches(anchor, evidence[anchor.key]):
            return True
    return False


def _descriptor_has_required_evidence(
    descriptor: CompiledDeviceDescriptor,
    evidence: Mapping[str, object],
) -> bool:
    return all(
        not anchor.required or anchor.key in evidence
        for anchor in descriptor.anchors
    )


def _anchor_matches(anchor: DetectionAnchorCondition, value: object) -> bool:
    return anchor_condition_matches(anchor, value)
