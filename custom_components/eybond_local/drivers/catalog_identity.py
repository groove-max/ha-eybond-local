"""Execute compiled catalog identity actions over immutable Modbus registers."""

from __future__ import annotations

from dataclasses import dataclass, replace
import asyncio
import logging
from typing import Any

from ..metadata.device_catalog_loader import (
    MATCH_DEVICE,
    MATCH_FAMILY,
    MATCH_NO_DATA,
    MATCH_UNIDENTIFIED,
    DeviceCatalogMatch,
    force_unsupported_models,
    load_device_catalog,
    serial_ascii_plausible,
)
from ..metadata.compiled_detection_catalog import (
    PROBE_ACTION_MODBUS_READ,
    CompiledResolutionResult,
    RESOLUTION_UNRESOLVED,
    load_compiled_detection_catalog,
)
from ..metadata.detection_decision_tree import evaluate_detection_decision_tree
from ..metadata.detection_evidence import (
    build_descriptor_decision_report_from_catalog_identity_probe,
)
from .catalog_probe import async_walk_detection_dag

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
    compiled_resolution: CompiledResolutionResult | None = None
    probe_action_keys: tuple[str, ...] = ()
    failed_probe_action_keys: tuple[str, ...] = ()

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
        if self.compiled_resolution is not None:
            payload["compiled_resolution"] = {
                "protocol_key": self.compiled_resolution.protocol_key,
                "resolution": self.compiled_resolution.resolution,
                "candidate_keys": list(self.compiled_resolution.candidate_keys),
                "surface_key": self.compiled_resolution.surface_key,
                "confidence": self.compiled_resolution.confidence,
                "missing_evidence_keys": list(
                    self.compiled_resolution.missing_evidence_keys
                ),
                "collected_evidence_keys": list(
                    self.compiled_resolution.collected_evidence_keys
                ),
                "failed_evidence_keys": list(
                    self.compiled_resolution.failed_evidence_keys
                ),
                "unsupported_evidence_keys": list(
                    self.compiled_resolution.unsupported_evidence_keys
                ),
                "contradicting_evidence_keys": list(
                    self.compiled_resolution.contradicting_evidence_keys
                ),
                "decision_path": list(self.compiled_resolution.decision_path),
                "catalog_version": self.compiled_resolution.catalog_version,
                "descriptor_revisions": list(
                    self.compiled_resolution.descriptor_revisions
                ),
                "evidence_fingerprint": self.compiled_resolution.evidence_fingerprint,
            }
        payload["probe_actions"] = {
            "executed": list(self.probe_action_keys),
            "failed": list(self.failed_probe_action_keys),
        }
        descriptor_report = build_descriptor_decision_report_from_catalog_identity_probe(self)
        if descriptor_report is not None:
            payload["descriptor_decision"] = descriptor_report
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

    compiled_catalog = load_compiled_detection_catalog()
    protocol = next(
        (
            candidate
            for candidate in compiled_catalog.protocols.values()
            if candidate.transport_key == transport_key
        ),
        None,
    )
    if protocol is None:
        return None

    words: dict[int, int] = {}
    raw_fields: dict[str, object] = {}
    evidence: dict[str, object] = {}
    tree = compiled_catalog.decision_trees[protocol.key]

    async def _execute(action) -> str:
        if action.register is None or action.count is None:
            return "failed"
        values = None
        last_error: Exception | None = None
        for _attempt in range(action.retries + 1):
            try:
                request = session.read_holding(action.register, action.count)
                values = (
                    await asyncio.wait_for(request, timeout=action.timeout)
                    if action.timeout > 0
                    else await request
                )
                break
            except Exception as exc:  # pylint: disable=broad-except
                last_error = exc
        if values is None:
            logger.debug(
                "Catalog identity action failed action=%s error=%s",
                action.key,
                last_error,
            )
            return "failed"
        for index, value in enumerate(values):
            words[action.register + index] = int(value)
        for field in action.evidence_fields:
            value = _decode_compiled_field(field, words)
            if value is None:
                continue
            raw_fields[field.source_key] = value
            evidence[field.key] = value
        return "executed"

    walk = await async_walk_detection_dag(
        protocol=protocol,
        tree=tree,
        evidence=evidence,
        execute_action=_execute,
        supported_kinds=frozenset({PROBE_ACTION_MODBUS_READ}),
    )
    evaluation = walk.evaluation
    executed_actions = list(walk.executed_actions)
    failed_actions = list(walk.failed_actions)
    failed_evidence = set(walk.failed_evidence) | set(walk.unsupported_evidence)
    if not executed_actions:
        return None

    layout_code = _optional_int(raw_fields.get("layout_code"))
    model_code = _optional_int(raw_fields.get("model_code"))
    if layout_code is None or model_code is None:
        # The fingerprint registers themselves were unreadable: that is "no
        # opinion" (e.g. a layout that rejects this block read), NOT link-down.
        # Link-down is specifically a SUCCESSFUL read returning zeros.
        return None
    rated_power = _optional_int(raw_fields.get("rated_power"))
    serial_ascii = str(raw_fields.get("serial_ascii") or "")
    if layout_code == 0 and model_code == 0:
        return CatalogIdentityProbe(
            layout_code=layout_code,
            model_code=model_code,
            rated_power=rated_power,
            serial_ascii=serial_ascii,
            match=DeviceCatalogMatch(kind=MATCH_NO_DATA),
            compiled_resolution=CompiledResolutionResult(
                protocol_key=protocol.key,
                resolution=RESOLUTION_UNRESOLVED,
                candidate_keys=(),
                surface_key=None,
                confidence="none",
                collected_evidence_keys=tuple(sorted(evidence)),
                catalog_version=compiled_catalog.catalog_version,
            ),
            probe_action_keys=tuple(executed_actions),
            failed_probe_action_keys=tuple(failed_actions),
        )
    if serial_ascii:
        evidence["structural.serial_ascii_plausible"] = serial_ascii_plausible(
            serial_ascii
        )

    final_evaluation = evaluate_detection_decision_tree(
        tree,
        evidence,
        unavailable_evidence_keys=frozenset(failed_evidence),
    )
    if final_evaluation.status in {"resolved", "ambiguous"}:
        compiled_resolution = compiled_catalog.resolution_for_candidates(
            protocol_key=protocol.key,
            candidate_keys=final_evaluation.candidate_keys,
            evidence=evidence,
            decision_path=tuple(
                f"{step.anchor_key}={step.value!r}"
                for step in final_evaluation.path
            ),
        )
    else:
        compiled_resolution = CompiledResolutionResult(
            protocol_key=protocol.key,
            resolution=RESOLUTION_UNRESOLVED,
            candidate_keys=final_evaluation.candidate_keys,
            surface_key=None,
            confidence="none",
            collected_evidence_keys=tuple(sorted(evidence)),
            decision_path=tuple(
                f"{step.anchor_key}={step.value!r}"
                for step in final_evaluation.path
            ),
            catalog_version=compiled_catalog.catalog_version,
        )
    compiled_resolution = replace(
        compiled_resolution,
        missing_evidence_keys=(
            (final_evaluation.missing_anchor_key,)
            if final_evaluation.missing_anchor_key
            else ()
        ),
        failed_evidence_keys=tuple(sorted(failed_evidence)),
    )
    if force_unsupported_models():
        family_resolution = compiled_catalog.resolve_family(
            protocol_key=protocol.key,
            evidence=evidence,
        )
        if family_resolution.resolved:
            compiled_resolution = replace(
                family_resolution,
                decision_path=compiled_resolution.decision_path,
                failed_evidence_keys=tuple(sorted(failed_evidence)),
            )
    match = catalog_match_from_resolution(
        resolution=compiled_resolution,
        layout_code=layout_code,
        rated_power=rated_power,
        serial_ascii=serial_ascii,
    )
    return CatalogIdentityProbe(
        layout_code=layout_code,
        model_code=model_code,
        rated_power=rated_power,
        serial_ascii=serial_ascii,
        match=match,
        compiled_resolution=compiled_resolution,
        probe_action_keys=tuple(executed_actions),
        failed_probe_action_keys=tuple(failed_actions),
    )


def catalog_match_from_resolution(
    *,
    resolution: CompiledResolutionResult,
    layout_code: int,
    rated_power: int | None,
    serial_ascii: str,
) -> DeviceCatalogMatch:
    """Build backward-compatible diagnostics from the compiled resolution."""

    source = load_device_catalog()
    layout = next(
        (item for item in source.layouts if layout_code in item.layout_codes),
        None,
    )
    if resolution.resolution == "exact" and len(resolution.candidate_keys) == 1:
        entry = next(
            (
                item
                for item in source.devices
                if item.entry_key == resolution.candidate_keys[0]
            ),
            None,
        )
        if entry is not None:
            signals = ["layout_code", "model_code"]
            if (
                rated_power is not None
                and rated_power in entry.fingerprint.rated_power_one_of
            ):
                signals.append("rated_power")
            if (
                "serial_ascii_plausible" in entry.structural
                and serial_ascii_plausible(serial_ascii)
            ):
                signals.append("serial_ascii")
            return DeviceCatalogMatch(
                kind=MATCH_DEVICE,
                tier=entry.tier,
                entry=entry,
                entries=(entry,),
                layout=layout,
                confidence_signals=tuple(signals),
            )
    if resolution.resolution == "family" and resolution.surface_key:
        family_default = next(
            (
                item
                for item in source.family_defaults
                if item.surface_key == resolution.surface_key
            ),
            None,
        )
        if family_default is not None:
            return DeviceCatalogMatch(
                kind=MATCH_FAMILY,
                tier=family_default.tier,
                layout=layout,
                family_default=family_default,
                confidence_signals=("layout_code",),
            )
    entries = tuple(
        item
        for item in source.devices
        if item.entry_key in resolution.candidate_keys
    )
    return DeviceCatalogMatch(
        kind=MATCH_UNIDENTIFIED,
        entries=entries,
        layout=layout,
        confidence_signals=("ambiguous_fingerprint",) if len(entries) > 1 else (),
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


def _decode_compiled_field(field: Any, words: dict[int, int]) -> object | None:
    if field.decoder == "ascii":
        return _decode_ascii_field(field.register, field.words, words)
    return words.get(field.register)


def _decode_ascii_field(register: int, word_count: int, words: dict[int, int]) -> str:
    raw = bytearray()
    for offset in range(word_count):
        value = words.get(register + offset)
        if value is None:
            continue
        raw += int(value).to_bytes(2, "big")
    text = raw.decode("ascii", errors="replace")
    return "".join(char for char in text if char.isprintable() and char.isalnum())


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)
