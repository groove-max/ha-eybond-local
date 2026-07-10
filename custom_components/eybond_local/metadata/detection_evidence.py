"""Normalize collected identity data into descriptor-tree anchor evidence.

This module is intentionally pure: it does not perform Modbus reads and does
not import driver-layer probe classes. Callers can pass the existing
``CatalogIdentityProbe`` object, its serialized details dict, or a direct
mapping of identity fields. The output uses descriptor anchor keys understood by
``detection_decision_tree``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .detection_decision_tree import (
    build_detection_decision_tree,
    evaluate_detection_decision_tree,
    serialize_detection_decision_evaluation,
)
from .device_catalog_loader import serial_ascii_plausible


_FIELD_TO_ANCHOR_KEY: dict[str, str] = {
    "layout_code": "fingerprint.layout_code",
    "model_code": "fingerprint.model_code",
    "rated_power": "fingerprint.rated_power",
}


def anchor_evidence_from_catalog_identity_probe(probe: Any | None) -> dict[str, object]:
    """Build anchor evidence from a catalog identity probe-like object."""

    if probe is None:
        return {}
    evidence: dict[str, object] = {}
    for field_name, anchor_key in _FIELD_TO_ANCHOR_KEY.items():
        value = getattr(probe, field_name, None)
        if value is not None:
            evidence[anchor_key] = value
    serial_ascii = str(getattr(probe, "serial_ascii", "") or "")
    if serial_ascii:
        evidence["structural.serial_ascii_plausible"] = serial_ascii_plausible(serial_ascii)
    return evidence


def anchor_evidence_from_catalog_identity_details(
    details: Mapping[str, object] | None,
) -> dict[str, object]:
    """Build anchor evidence from serialized ``device_catalog`` details."""

    if not details:
        return {}
    evidence: dict[str, object] = {}
    for field_name, anchor_key in _FIELD_TO_ANCHOR_KEY.items():
        value = details.get(field_name)
        if value is not None:
            evidence[anchor_key] = value
    return evidence


def anchor_evidence_from_identity_mapping(
    values: Mapping[str, object] | None,
) -> dict[str, object]:
    """Build anchor evidence from raw or already-normalized identity fields."""

    if not values:
        return {}
    evidence: dict[str, object] = {}
    for field_name, anchor_key in _FIELD_TO_ANCHOR_KEY.items():
        raw_value = values.get(anchor_key, values.get(field_name))
        if raw_value is not None:
            evidence[anchor_key] = raw_value
    serial_ascii = str(values.get("serial_ascii", "") or "")
    if serial_ascii:
        evidence["structural.serial_ascii_plausible"] = serial_ascii_plausible(serial_ascii)
    structural_value = values.get("structural.serial_ascii_plausible")
    if structural_value is not None:
        evidence["structural.serial_ascii_plausible"] = bool(structural_value)
    return evidence


def combined_anchor_evidence(
    *parts: Mapping[str, object] | None,
) -> dict[str, object]:
    """Merge anchor evidence parts, ignoring empty values."""

    evidence: dict[str, object] = {}
    for part in parts:
        if part:
            evidence.update(part)
    return evidence


def build_descriptor_decision_report(
    *,
    protocol_family: str,
    evidence: Mapping[str, object],
    catalog_match_kind: str = "",
    catalog_entry_key: str = "",
) -> dict[str, object]:
    """Build a compact diagnostic report for descriptor-tree shadow evaluation."""

    tree = build_detection_decision_tree(protocol_family=protocol_family)
    evaluation = evaluate_detection_decision_tree(tree, evidence)
    serialized_evaluation = serialize_detection_decision_evaluation(evaluation)
    return {
        "kind": "descriptor_decision_shadow",
        "protocol_family": tree.protocol_family,
        "tree": {
            "descriptor_count": tree.descriptor_count,
            "ambiguous_leaf_count": tree.ambiguous_leaf_count,
            "max_depth": tree.max_depth,
            "anchor_keys": list(tree.anchor_keys),
        },
        "evidence": dict(evidence),
        "evaluation": serialized_evaluation,
        "catalog_match": {
            "kind": str(catalog_match_kind or ""),
            "entry_key": str(catalog_entry_key or ""),
        },
        "agreement": _descriptor_report_agreement(
            catalog_match_kind=str(catalog_match_kind or ""),
            catalog_entry_key=str(catalog_entry_key or ""),
            resolved_key=str(serialized_evaluation.get("resolved_key") or ""),
        ),
    }


def build_descriptor_decision_report_from_catalog_identity_probe(
    probe: Any | None,
    *,
    protocol_family: str = "modbus_smg",
) -> dict[str, object] | None:
    """Build a descriptor-tree report from a catalog identity probe-like object."""

    if probe is None:
        return None
    evidence = anchor_evidence_from_catalog_identity_probe(probe)
    match = getattr(probe, "match", None)
    entry = getattr(match, "entry", None)
    return build_descriptor_decision_report(
        protocol_family=protocol_family,
        evidence=evidence,
        catalog_match_kind=str(getattr(match, "kind", "") or ""),
        catalog_entry_key=str(getattr(entry, "entry_key", "") or ""),
    )


def _descriptor_report_agreement(
    *,
    catalog_match_kind: str,
    catalog_entry_key: str,
    resolved_key: str,
) -> str:
    if not resolved_key:
        return "unresolved"
    if catalog_match_kind == "device":
        return "match" if catalog_entry_key and catalog_entry_key == resolved_key else "mismatch"
    if catalog_match_kind == "family":
        return "match" if resolved_key.endswith(".family_fallback") else "mismatch"
    if catalog_match_kind in {"no_data", "unidentified"}:
        return "not_applicable"
    return "unknown"
