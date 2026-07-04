"""Build and evaluate a read-only descriptor-driven detection decision tree."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .detection_descriptor_loader import (
    DetectionAnchorCondition,
    DetectionDescriptorCatalog,
    DetectionDeviceDescriptor,
    detection_anchor_cost,
    load_detection_descriptor_catalog,
    mandatory_detection_anchor_keys,
    _stable_signature_value,
)
from .anchor_matching import (
    contains_any_matches,
    known_enum_matches,
    range_matches,
)


@dataclass(frozen=True, slots=True)
class DetectionDecisionLeaf:
    """Terminal decision state for one descriptor candidate set."""

    candidate_keys: tuple[str, ...]
    resolved_key: str | None
    validation_anchors: tuple[DetectionAnchorCondition, ...] = ()

    @property
    def ambiguous(self) -> bool:
        """Return whether more than one candidate remains."""

        return len(self.candidate_keys) != 1


@dataclass(frozen=True, slots=True)
class DetectionDecisionBranch:
    """One anchor result branch in a detection decision tree."""

    signature: str
    candidate_keys: tuple[str, ...]
    priority: int
    child: "DetectionDecisionNode | DetectionDecisionLeaf"


@dataclass(frozen=True, slots=True)
class DetectionDecisionNode:
    """One anchor read/evaluation point in a detection decision tree."""

    anchor_key: str
    cost: int
    candidate_keys: tuple[str, ...]
    partition_count: int
    score: float
    forced: bool
    branches: tuple[DetectionDecisionBranch, ...]


@dataclass(frozen=True, slots=True)
class DetectionDecisionTree:
    """Static detection decision tree for one protocol family."""

    protocol_family: str
    descriptor_count: int
    root: DetectionDecisionNode | DetectionDecisionLeaf

    @property
    def ambiguous_leaf_count(self) -> int:
        """Return the number of terminal ambiguous leaves."""

        return _count_ambiguous_leaves(self.root)

    @property
    def max_depth(self) -> int:
        """Return maximum anchor depth in the tree."""

        return _max_depth(self.root)

    @property
    def anchor_keys(self) -> tuple[str, ...]:
        """Return anchor keys used anywhere in first-seen traversal order."""

        keys: list[str] = []
        seen: set[str] = set()
        _collect_anchor_keys(self.root, keys, seen)
        return tuple(keys)


@dataclass(frozen=True, slots=True)
class DetectionDecisionEvaluationStep:
    """One evidence-driven traversal step through a decision tree."""

    anchor_key: str
    value: object
    matched_signature: str
    candidate_keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DetectionDecisionEvaluation:
    """Result of applying observed anchor evidence to a decision tree."""

    status: str
    candidate_keys: tuple[str, ...]
    resolved_key: str | None = None
    missing_anchor_key: str | None = None
    path: tuple[DetectionDecisionEvaluationStep, ...] = ()

    @property
    def resolved(self) -> bool:
        """Return whether exactly one descriptor was resolved."""

        return self.status == "resolved" and self.resolved_key is not None


def build_detection_decision_tree(
    *,
    protocol_family: str,
    catalog: DetectionDescriptorCatalog | None = None,
) -> DetectionDecisionTree:
    """Build a deterministic static decision tree for one protocol family."""

    resolved = catalog if catalog is not None else load_detection_descriptor_catalog()
    descriptors = resolved.descriptors_for_protocol(protocol_family)
    root = _build_node(
        descriptors=descriptors,
        used_anchor_keys=frozenset(),
        forced_anchor_keys=mandatory_detection_anchor_keys(protocol_family),
    )
    return DetectionDecisionTree(
        protocol_family=str(protocol_family or "").strip(),
        descriptor_count=len(descriptors),
        root=root,
    )


def serialize_detection_decision_tree(tree: DetectionDecisionTree) -> dict[str, object]:
    """Return a JSON-safe diagnostic representation of one decision tree."""

    return {
        "protocol_family": tree.protocol_family,
        "descriptor_count": tree.descriptor_count,
        "ambiguous_leaf_count": tree.ambiguous_leaf_count,
        "max_depth": tree.max_depth,
        "anchor_keys": list(tree.anchor_keys),
        "root": _serialize_node(tree.root),
    }


def serialize_detection_decision_evaluation(
    evaluation: DetectionDecisionEvaluation,
) -> dict[str, object]:
    """Return a JSON-safe diagnostic representation of one tree evaluation."""

    return {
        "status": evaluation.status,
        "candidate_keys": list(evaluation.candidate_keys),
        "resolved_key": evaluation.resolved_key,
        "missing_anchor_key": evaluation.missing_anchor_key,
        "path": [
            {
                "anchor_key": step.anchor_key,
                "value": step.value,
                "matched_signature": step.matched_signature,
                "candidate_keys": list(step.candidate_keys),
            }
            for step in evaluation.path
        ],
    }


def evaluate_detection_decision_tree_static(
    tree: "DetectionDecisionTree",
    evidence: Mapping[str, object],
) -> "DetectionDecisionEvaluation":
    """Evaluate a tree against a FIXED evidence dict (no more probing).

    The interactive walkers answer ``missing_anchor`` by executing another
    probe action; a one-shot resolver over already-collected evidence must
    instead declare each missing key unavailable and let the tree route
    around it, exactly as the walkers do for unsupported actions.
    """

    unavailable: set[str] = set()
    while True:
        evaluation = evaluate_detection_decision_tree(
            tree,
            evidence,
            unavailable_evidence_keys=frozenset(unavailable),
        )
        if evaluation.status != "missing_anchor" or not evaluation.missing_anchor_key:
            return evaluation
        unavailable.add(evaluation.missing_anchor_key)


def evaluate_detection_decision_tree(
    tree: DetectionDecisionTree,
    evidence: Mapping[str, object],
    *,
    unavailable_evidence_keys: frozenset[str] = frozenset(),
) -> DetectionDecisionEvaluation:
    """Evaluate one decision tree against already collected anchor evidence."""

    return _evaluate_node(tree.root, evidence, (), unavailable_evidence_keys)


def _build_node(
    *,
    descriptors: tuple[DetectionDeviceDescriptor, ...],
    used_anchor_keys: frozenset[str],
    forced_anchor_keys: tuple[str, ...],
) -> DetectionDecisionNode | DetectionDecisionLeaf:
    candidate_keys = tuple(descriptor.key for descriptor in descriptors)
    if len(descriptors) <= 1:
        descriptor = descriptors[0] if descriptors else None
        return DetectionDecisionLeaf(
            candidate_keys=candidate_keys,
            resolved_key=candidate_keys[0] if candidate_keys else None,
            validation_anchors=(
                tuple(
                    anchor
                    for anchor in descriptor.anchors
                    if anchor.key not in used_anchor_keys
                )
                if descriptor is not None
                else ()
            ),
        )

    forced_anchor_key = _next_forced_anchor_key(forced_anchor_keys, used_anchor_keys)
    if forced_anchor_key is not None:
        return _build_anchor_node(
            descriptors=descriptors,
            used_anchor_keys=used_anchor_keys,
            forced_anchor_keys=forced_anchor_keys,
            anchor_key=forced_anchor_key,
            forced=True,
        )

    best_anchor_key = _best_branch_anchor_key(descriptors, used_anchor_keys)
    if best_anchor_key is None:
        return DetectionDecisionLeaf(candidate_keys=candidate_keys, resolved_key=None)

    return _build_anchor_node(
        descriptors=descriptors,
        used_anchor_keys=used_anchor_keys,
        forced_anchor_keys=forced_anchor_keys,
        anchor_key=best_anchor_key,
        forced=False,
    )


def _build_anchor_node(
    *,
    descriptors: tuple[DetectionDeviceDescriptor, ...],
    used_anchor_keys: frozenset[str],
    forced_anchor_keys: tuple[str, ...],
    anchor_key: str,
    forced: bool,
) -> DetectionDecisionNode:
    partitions = _partition_descriptors_by_anchor(descriptors, anchor_key)
    next_used_anchor_keys = frozenset((*used_anchor_keys, anchor_key))
    branches = tuple(
        DetectionDecisionBranch(
            signature=signature,
            candidate_keys=tuple(descriptor.key for descriptor in branch_descriptors),
            priority=max(descriptor.priority for descriptor in branch_descriptors),
            child=_build_node(
                descriptors=branch_descriptors,
                used_anchor_keys=next_used_anchor_keys,
                forced_anchor_keys=forced_anchor_keys,
            ),
        )
        for signature, branch_descriptors in sorted(partitions.items())
    )
    cost = detection_anchor_cost(anchor_key)
    partition_count = len(partitions)
    score = 0.0
    if partition_count > 1:
        score = (partition_count - 1) * len(descriptors) / cost
    return DetectionDecisionNode(
        anchor_key=anchor_key,
        cost=cost,
        candidate_keys=tuple(descriptor.key for descriptor in descriptors),
        partition_count=partition_count,
        score=score,
        forced=forced,
        branches=branches,
    )


def _next_forced_anchor_key(
    forced_anchor_keys: tuple[str, ...],
    used_anchor_keys: frozenset[str],
) -> str | None:
    for anchor_key in forced_anchor_keys:
        if anchor_key not in used_anchor_keys:
            return anchor_key
    return None


def _best_branch_anchor_key(
    descriptors: tuple[DetectionDeviceDescriptor, ...],
    used_anchor_keys: frozenset[str],
) -> str | None:
    best: tuple[float, int, int, str] | None = None
    for anchor_key in _candidate_anchor_keys(descriptors):
        if anchor_key in used_anchor_keys:
            continue
        partitions = _partition_descriptors_by_anchor(descriptors, anchor_key)
        if len(partitions) <= 1:
            continue
        cost = detection_anchor_cost(anchor_key)
        score = (len(partitions) - 1) * len(descriptors) / cost
        candidate = (score, len(partitions), -cost, anchor_key)
        if best is None or candidate > best:
            best = candidate
    return best[3] if best is not None else None


def _candidate_anchor_keys(
    descriptors: tuple[DetectionDeviceDescriptor, ...],
) -> tuple[str, ...]:
    keys: set[str] = set()
    for descriptor in descriptors:
        keys.update(anchor.key for anchor in descriptor.anchors if anchor.required)
    return tuple(sorted(keys))


def _partition_descriptors_by_anchor(
    descriptors: tuple[DetectionDeviceDescriptor, ...],
    anchor_key: str,
) -> dict[str, tuple[DetectionDeviceDescriptor, ...]]:
    partitions: dict[str, list[DetectionDeviceDescriptor]] = {}
    for descriptor in descriptors:
        signature = _anchor_signature(descriptor, anchor_key)
        partitions.setdefault(signature, []).append(descriptor)
    return {
        signature: tuple(items)
        for signature, items in partitions.items()
    }


def _anchor_signature(descriptor: DetectionDeviceDescriptor, anchor_key: str) -> str:
    for anchor in descriptor.anchors:
        if anchor.key == anchor_key:
            return _condition_signature(anchor)
    return "missing"


def _condition_signature(anchor: DetectionAnchorCondition) -> str:
    if anchor.equals is not None:
        return f"equals:{_stable_signature_value(anchor.equals)}"
    if anchor.one_of:
        values = ",".join(_stable_signature_value(value) for value in anchor.one_of)
        return f"one_of:{values}"
    if anchor.min_value is not None or anchor.max_value is not None:
        return f"range:{anchor.min_value}:{anchor.max_value}"
    if anchor.known_enum:
        return "known_enum"
    if anchor.contains_any:
        return "contains_any:" + ",".join(anchor.contains_any)
    return "present"


def _count_ambiguous_leaves(
    node: DetectionDecisionNode | DetectionDecisionLeaf,
) -> int:
    if isinstance(node, DetectionDecisionLeaf):
        return 1 if node.ambiguous else 0
    return sum(_count_ambiguous_leaves(branch.child) for branch in node.branches)


def _max_depth(node: DetectionDecisionNode | DetectionDecisionLeaf) -> int:
    if isinstance(node, DetectionDecisionLeaf):
        return 0
    if not node.branches:
        return 1
    return 1 + max(_max_depth(branch.child) for branch in node.branches)


def _collect_anchor_keys(
    node: DetectionDecisionNode | DetectionDecisionLeaf,
    keys: list[str],
    seen: set[str],
) -> None:
    if isinstance(node, DetectionDecisionLeaf):
        return
    if node.anchor_key not in seen:
        seen.add(node.anchor_key)
        keys.append(node.anchor_key)
    for branch in node.branches:
        _collect_anchor_keys(branch.child, keys, seen)


def _serialize_node(
    node: DetectionDecisionNode | DetectionDecisionLeaf,
) -> dict[str, object]:
    if isinstance(node, DetectionDecisionLeaf):
        return {
            "type": "leaf",
            "candidate_keys": list(node.candidate_keys),
            "resolved_key": node.resolved_key,
            "ambiguous": node.ambiguous,
        }
    return {
        "type": "node",
        "anchor_key": node.anchor_key,
        "cost": node.cost,
        "candidate_keys": list(node.candidate_keys),
        "partition_count": node.partition_count,
        "score": node.score,
        "forced": node.forced,
        "branches": [
            {
                "signature": branch.signature,
                "candidate_keys": list(branch.candidate_keys),
                "priority": branch.priority,
                "child": _serialize_node(branch.child),
            }
            for branch in node.branches
        ],
    }


def _evaluate_node(
    node: DetectionDecisionNode | DetectionDecisionLeaf,
    evidence: Mapping[str, object],
    path: tuple[DetectionDecisionEvaluationStep, ...],
    unavailable_evidence_keys: frozenset[str],
) -> DetectionDecisionEvaluation:
    if isinstance(node, DetectionDecisionLeaf):
        if node.resolved_key is not None and not node.ambiguous:
            validation = _evaluate_leaf_validation(
                node,
                evidence,
                unavailable_evidence_keys,
            )
            if validation is not None:
                if validation.status == "missing_anchor":
                    return DetectionDecisionEvaluation(
                        status="missing_anchor",
                        candidate_keys=node.candidate_keys,
                        missing_anchor_key=validation.missing_anchor_key,
                        path=path,
                    )
                return DetectionDecisionEvaluation(
                    status="no_match",
                    candidate_keys=node.candidate_keys,
                    path=path,
                )
            return DetectionDecisionEvaluation(
                status="resolved",
                candidate_keys=node.candidate_keys,
                resolved_key=node.resolved_key,
                path=path,
            )
        if node.candidate_keys:
            return DetectionDecisionEvaluation(
                status="ambiguous",
                candidate_keys=node.candidate_keys,
                path=path,
            )
        return DetectionDecisionEvaluation(status="no_match", candidate_keys=(), path=path)

    if node.anchor_key not in evidence:
        if node.anchor_key in unavailable_evidence_keys:
            missing_branch = next(
                (branch for branch in node.branches if branch.signature == "missing"),
                None,
            )
            if missing_branch is not None:
                return _evaluate_node(
                    missing_branch.child,
                    evidence,
                    path,
                    unavailable_evidence_keys,
                )
            return DetectionDecisionEvaluation(
                status="no_match",
                candidate_keys=node.candidate_keys,
                path=path,
            )
        return DetectionDecisionEvaluation(
            status="missing_anchor",
            candidate_keys=node.candidate_keys,
            missing_anchor_key=node.anchor_key,
            path=path,
        )

    value = evidence[node.anchor_key]
    for branch in _matching_branches_by_priority(node.branches, value):
        step = DetectionDecisionEvaluationStep(
            anchor_key=node.anchor_key,
            value=value,
            matched_signature=branch.signature,
            candidate_keys=branch.candidate_keys,
        )
        result = _evaluate_node(
            branch.child,
            evidence,
            (*path, step),
            unavailable_evidence_keys,
        )
        if result.status != "no_match":
            return result
    return DetectionDecisionEvaluation(
        status="no_match",
        candidate_keys=node.candidate_keys,
        path=path,
    )


def _matching_branches_by_priority(
    branches: tuple[DetectionDecisionBranch, ...],
    value: object,
) -> tuple[DetectionDecisionBranch, ...]:
    selected: list[DetectionDecisionBranch] = []
    for matcher in (
        _signature_equals_value,
        _signature_range_contains_value,
        _signature_one_of_contains_value,
        _signature_contains_any,
        _signature_known_or_present,
    ):
        matches = tuple(branch for branch in branches if matcher(branch.signature, value))
        if matches:
            highest_priority = max(branch.priority for branch in matches)
            prioritized = tuple(
                branch for branch in matches if branch.priority == highest_priority
            )
            if len(prioritized) == 1:
                selected.append(prioritized[0])
            else:
                selected.append(_ambiguous_branch(prioritized))
    selected.extend(branch for branch in branches if branch.signature == "missing")
    return tuple(selected)


def _evaluate_leaf_validation(
    leaf: DetectionDecisionLeaf,
    evidence: Mapping[str, object],
    unavailable_evidence_keys: frozenset[str],
) -> DetectionDecisionEvaluation | None:
    for anchor in leaf.validation_anchors:
        if anchor.key not in evidence:
            if anchor.required:
                if anchor.key in unavailable_evidence_keys:
                    return DetectionDecisionEvaluation(
                        status="no_match",
                        candidate_keys=leaf.candidate_keys,
                    )
                return DetectionDecisionEvaluation(
                    status="missing_anchor",
                    candidate_keys=leaf.candidate_keys,
                    missing_anchor_key=anchor.key,
                )
            continue
        if not _signature_matches_value(_condition_signature(anchor), evidence[anchor.key]):
            return DetectionDecisionEvaluation(
                status="no_match",
                candidate_keys=leaf.candidate_keys,
            )
    return None


def _ambiguous_branch(
    branches: tuple[DetectionDecisionBranch, ...],
) -> DetectionDecisionBranch:
    candidate_keys = tuple(
        dict.fromkeys(
            candidate_key
            for branch in branches
            for candidate_key in branch.candidate_keys
        )
    )
    return DetectionDecisionBranch(
        signature="ambiguous",
        candidate_keys=candidate_keys,
        priority=max(branch.priority for branch in branches),
        child=DetectionDecisionLeaf(candidate_keys=candidate_keys, resolved_key=None),
    )


def _signature_equals_value(signature: str, value: object) -> bool:
    prefix = "equals:"
    if not signature.startswith(prefix):
        return False
    return signature.removeprefix(prefix) == _stable_signature_value(value)


def _signature_one_of_contains_value(signature: str, value: object) -> bool:
    prefix = "one_of:"
    if not signature.startswith(prefix):
        return False
    values = tuple(item for item in signature.removeprefix(prefix).split(",") if item)
    return _stable_signature_value(value) in values


def _signature_range_contains_value(signature: str, value: object) -> bool:
    prefix = "range:"
    if not signature.startswith(prefix):
        return False
    _range, min_value, max_value = signature.split(":", 2)
    return range_matches(
        None if min_value == "None" else float(min_value),
        None if max_value == "None" else float(max_value),
        value,
    )


def _signature_known_or_present(signature: str, value: object) -> bool:
    if signature == "known_enum":
        # Strict, matching the family-fallback contradiction checks: the
        # value must be a decoded enum label, not an "Unknown (n)"
        # placeholder. Accepting any non-None value here would validate
        # garbage that the other evaluator rejects.
        return known_enum_matches(value)
    if signature == "present":
        return value is not None
    return False


def _signature_contains_any(signature: str, value: object) -> bool:
    prefix = "contains_any:"
    if not signature.startswith(prefix):
        return False
    return contains_any_matches(
        tuple(signature.removeprefix(prefix).split(",")), value
    )


def _signature_matches_value(signature: str, value: object) -> bool:
    return any(
        matcher(signature, value)
        for matcher in (
            _signature_equals_value,
            _signature_range_contains_value,
            _signature_one_of_contains_value,
            _signature_contains_any,
            _signature_known_or_present,
        )
    )
