"""Execute compiled read-only detection plans through the decision DAG."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
import asyncio
from typing import Any

from ..metadata.compiled_detection_catalog import (
    PROBE_ACTION_ASCII_COMMAND,
    PROBE_ACTION_COLLECTOR_METADATA,
    PROBE_ACTION_SMARTESS_QUERY,
    CompiledProbeAction,
    CompiledProtocolDescriptor,
    CompiledResolutionResult,
    RESOLUTION_UNRESOLVED,
    load_compiled_detection_catalog,
)
from ..metadata.detection_decision_tree import (
    DetectionDecisionEvaluation,
    evaluate_detection_decision_tree,
)


Parser = Callable[[str], dict[str, Any]]
EvidenceProvider = Callable[[CompiledProbeAction], Awaitable[object]]


@dataclass(frozen=True, slots=True)
class CatalogAsciiProbe:
    """Collected values, evidence audit, and catalog resolution."""

    values: dict[str, Any]
    evidence: dict[str, object]
    resolution: CompiledResolutionResult
    executed_actions: tuple[str, ...]
    failed_actions: tuple[str, ...]
    unsupported_actions: tuple[str, ...] = ()

    def as_details(self) -> dict[str, object]:
        """Return a stable support/persistence payload."""

        return {
            "protocol_key": self.resolution.protocol_key,
            "resolution": self.resolution.resolution,
            "candidate_keys": list(self.resolution.candidate_keys),
            "surface_key": self.resolution.surface_key,
            "confidence": self.resolution.confidence,
            "catalog_version": self.resolution.catalog_version,
            "descriptor_revisions": list(self.resolution.descriptor_revisions),
            "evidence_fingerprint": self.resolution.evidence_fingerprint,
            "collected_evidence_keys": list(
                self.resolution.collected_evidence_keys
            ),
            "missing_evidence_keys": list(self.resolution.missing_evidence_keys),
            "failed_evidence_keys": list(self.resolution.failed_evidence_keys),
            "unsupported_evidence_keys": list(
                self.resolution.unsupported_evidence_keys
            ),
            "contradicting_evidence_keys": list(
                self.resolution.contradicting_evidence_keys
            ),
            "decision_path": list(self.resolution.decision_path),
            "probe_actions": {
                "executed": list(self.executed_actions),
                "failed": list(self.failed_actions),
                "unsupported": list(self.unsupported_actions),
            },
        }


async def async_probe_ascii_catalog(
    *,
    protocol_key: str,
    session: Any,
    parsers: Mapping[str, Parser],
    collector: Any = None,
    evidence_providers: Mapping[str, EvidenceProvider] | None = None,
) -> CatalogAsciiProbe:
    """Execute only the actions required by the compiled decision DAG."""

    catalog = load_compiled_detection_catalog()
    protocol = catalog.protocols[protocol_key]
    tree = catalog.decision_trees[protocol_key]
    values: dict[str, Any] = {}
    evidence: dict[str, object] = {}
    executed: list[str] = []
    failed: list[str] = []
    unsupported: list[str] = []
    failed_evidence: set[str] = set()
    unsupported_evidence: set[str] = set()

    for action in protocol.probe_actions:
        if action.kind == PROBE_ACTION_COLLECTOR_METADATA:
            _collect_metadata_evidence(action, collector, values, evidence)
            executed.append(action.key)

    while True:
        unavailable = frozenset((*failed_evidence, *unsupported_evidence))
        evaluation = evaluate_detection_decision_tree(
            tree,
            evidence,
            unavailable_evidence_keys=unavailable,
        )
        if evaluation.status != "missing_anchor":
            break
        action = action_for_evidence_key(
            protocol,
            evaluation.missing_anchor_key or "",
            excluded_action_keys=frozenset((*executed, *failed, *unsupported)),
        )
        if action is None:
            unsupported_evidence.add(evaluation.missing_anchor_key or "")
            continue
        outcome = await _execute_probe_action(
            action,
            session=session,
            parsers=parsers,
            values=values,
            evidence=evidence,
            evidence_providers=evidence_providers or {},
        )
        if outcome == "executed":
            executed.append(action.key)
        elif outcome == "unsupported":
            unsupported.append(action.key)
            unsupported_evidence.update(field.key for field in action.evidence_fields)
        else:
            failed.append(action.key)
            failed_evidence.update(field.key for field in action.evidence_fields)
            if not action.optional:
                raise RuntimeError(f"required_catalog_action_failed:{action.key}")

    for action in protocol.probe_actions:
        if action.optional or action.key in executed:
            continue
        if action.kind == PROBE_ACTION_COLLECTOR_METADATA:
            continue
        outcome = await _execute_probe_action(
            action,
            session=session,
            parsers=parsers,
            values=values,
            evidence=evidence,
            evidence_providers=evidence_providers or {},
        )
        if outcome == "executed":
            executed.append(action.key)
        elif outcome == "unsupported":
            unsupported.append(action.key)
            unsupported_evidence.update(field.key for field in action.evidence_fields)
            raise RuntimeError(f"required_catalog_action_unsupported:{action.key}")
        else:
            failed.append(action.key)
            failed_evidence.update(field.key for field in action.evidence_fields)
            raise RuntimeError(f"required_catalog_action_failed:{action.key}")

    final_evaluation = evaluate_detection_decision_tree(
        tree,
        evidence,
        unavailable_evidence_keys=frozenset(
            (*failed_evidence, *unsupported_evidence)
        ),
    )
    resolution = _resolution_from_evaluation(
        protocol_key=protocol_key,
        evaluation=final_evaluation,
        evidence=evidence,
    )
    resolution = replace(
        resolution,
        missing_evidence_keys=(
            (final_evaluation.missing_anchor_key,)
            if final_evaluation.missing_anchor_key
            else ()
        ),
        failed_evidence_keys=tuple(sorted(failed_evidence)),
        unsupported_evidence_keys=tuple(sorted(unsupported_evidence)),
    )
    return CatalogAsciiProbe(
        values=values,
        evidence=evidence,
        resolution=resolution,
        executed_actions=tuple(executed),
        failed_actions=tuple(failed),
        unsupported_actions=tuple(unsupported),
    )


async def async_probe_ascii_catalog_signature(
    *,
    protocol_key: str,
    session: Any,
    parsers: Mapping[str, Parser],
) -> bool:
    """Execute the decision-tree root action as a cheap protocol signature."""

    catalog = load_compiled_detection_catalog()
    protocol = catalog.protocols[protocol_key]
    tree = catalog.decision_trees[protocol_key]
    root_key = getattr(tree.root, "anchor_key", "")
    action = action_for_evidence_key(protocol, root_key)
    if action is None or action.kind != PROBE_ACTION_ASCII_COMMAND:
        return False
    values: dict[str, Any] = {}
    evidence: dict[str, object] = {}
    outcome = await _execute_probe_action(
        action,
        session=session,
        parsers=parsers,
        values=values,
        evidence=evidence,
        evidence_providers={},
    )
    if outcome != "executed":
        return False
    evaluation = evaluate_detection_decision_tree(tree, evidence)
    return evaluation.status != "no_match"


def action_for_evidence_key(
    protocol: CompiledProtocolDescriptor,
    evidence_key: str,
    *,
    excluded_action_keys: frozenset[str] = frozenset(),
) -> CompiledProbeAction | None:
    """Return the cheapest unexecuted action producing one evidence key."""

    candidates = tuple(
        action
        for action in protocol.probe_actions
        if action.key not in excluded_action_keys
        and any(field.key == evidence_key for field in action.evidence_fields)
    )
    if not candidates:
        return None
    return min(candidates, key=lambda action: (action.cost, action.key))


def catalog_model_name(
    *,
    protocol_key: str,
    resolution: CompiledResolutionResult,
    values: Mapping[str, object],
) -> str:
    """Build a display name from the resolved descriptor and local evidence."""

    catalog = load_compiled_detection_catalog()
    descriptors = tuple(
        catalog.devices[key]
        for key in resolution.candidate_keys
        if key in catalog.devices
    )
    model_names = {descriptor.model_name for descriptor in descriptors}
    base_name = (
        next(iter(model_names))
        if len(model_names) == 1
        else protocol_key.upper()
    )
    if base_name not in {"PI30", "PI18", "PI41"}:
        return base_name
    model_number = values.get("model_number")
    if isinstance(model_number, str) and model_number.strip():
        return model_number.strip()
    rated_power = values.get("output_rating_active_power")
    if isinstance(rated_power, int) and rated_power > 0:
        return f"{base_name} {rated_power}"
    return base_name


async def _execute_probe_action(
    action: CompiledProbeAction,
    *,
    session: Any,
    parsers: Mapping[str, Parser],
    values: dict[str, Any],
    evidence: dict[str, object],
    evidence_providers: Mapping[str, EvidenceProvider],
) -> str:
    if action.kind == PROBE_ACTION_SMARTESS_QUERY:
        return await _execute_evidence_provider(
            action,
            provider=evidence_providers.get(action.parser_key),
            values=values,
            evidence=evidence,
        )
    if action.kind != PROBE_ACTION_ASCII_COMMAND:
        return "unsupported"
    parser = parsers.get(action.parser_key)
    if parser is None:
        return "unsupported"
    parsed = None
    for _attempt in range(action.retries + 1):
        try:
            request = session.request(action.command)
            payload = (
                await asyncio.wait_for(request, timeout=action.timeout)
                if action.timeout > 0
                else await request
            )
            parsed = parser(payload)
            break
        except Exception:
            continue
    if parsed is None:
        return "failed"
    values.update(parsed)
    for field in action.evidence_fields:
        if field.source_key in parsed:
            evidence[field.key] = parsed[field.source_key]
    return "executed"


async def _execute_evidence_provider(
    action: CompiledProbeAction,
    *,
    provider: EvidenceProvider | None,
    values: dict[str, Any],
    evidence: dict[str, object],
) -> str:
    if provider is None:
        return "unsupported"
    result: object | None = None
    for _attempt in range(action.retries + 1):
        try:
            request = provider(action)
            result = (
                await asyncio.wait_for(request, timeout=action.timeout)
                if action.timeout > 0
                else await request
            )
            break
        except Exception:
            continue
    if result is None:
        return "failed"
    payload = result if isinstance(result, Mapping) else {}
    for field in action.evidence_fields:
        value = payload.get(field.source_key)
        if value is None and len(action.evidence_fields) == 1 and not payload:
            value = result
        if value in (None, ""):
            continue
        values[field.source_key] = value
        evidence[field.key] = value
    return "executed"


def evidence_providers_from_transport(
    transport: Any,
) -> Mapping[str, EvidenceProvider]:
    """Return optional catalog evidence providers exposed by a transport."""

    providers = getattr(transport, "detection_evidence_providers", None)
    return providers if isinstance(providers, Mapping) else {}


def _collect_metadata_evidence(action, collector, values, evidence) -> None:
    if collector is None:
        return
    for field in action.evidence_fields:
        value = getattr(collector, field.source_key, None)
        if value in (None, ""):
            continue
        values[field.source_key] = value
        evidence[field.key] = value


def _resolution_from_evaluation(
    *,
    protocol_key: str,
    evaluation: DetectionDecisionEvaluation,
    evidence: Mapping[str, object],
) -> CompiledResolutionResult:
    catalog = load_compiled_detection_catalog()
    if evaluation.status in {"resolved", "ambiguous"}:
        return catalog.resolution_for_candidates(
            protocol_key=protocol_key,
            candidate_keys=evaluation.candidate_keys,
            evidence=evidence,
            decision_path=tuple(
                f"{step.anchor_key}={step.value!r}" for step in evaluation.path
            ),
        )
    return CompiledResolutionResult(
        protocol_key=protocol_key,
        resolution=RESOLUTION_UNRESOLVED,
        candidate_keys=evaluation.candidate_keys,
        surface_key=None,
        confidence="none",
        collected_evidence_keys=tuple(sorted(evidence)),
        decision_path=tuple(
            f"{step.anchor_key}={step.value!r}" for step in evaluation.path
        ),
        catalog_version=catalog.catalog_version,
    )
