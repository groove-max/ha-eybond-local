"""Deterministic vetting of a device contribution record before promotion.

Each check returns pass / warn / fail. A record only auto-promotes when there
are no failures; warnings are for human review (new layout, already-supported
device, sparse label evidence).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..metadata.device_catalog_loader import DeviceCatalog, load_device_catalog
from .contribution_record import RECORD_VERSION, record_contains_identifier


RESULT_PASS = "pass"
RESULT_WARN = "warn"
RESULT_FAIL = "fail"


@dataclass(frozen=True, slots=True)
class VetCheck:
    """One named vetting check outcome."""

    name: str
    result: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class VetReport:
    """All checks for one contribution record plus the overall verdict."""

    checks: tuple[VetCheck, ...] = field(default_factory=tuple)

    @property
    def failed(self) -> tuple[VetCheck, ...]:
        return tuple(check for check in self.checks if check.result == RESULT_FAIL)

    @property
    def warnings(self) -> tuple[VetCheck, ...]:
        return tuple(check for check in self.checks if check.result == RESULT_WARN)

    @property
    def verdict(self) -> str:
        if self.failed:
            return RESULT_FAIL
        if self.warnings:
            return RESULT_WARN
        return RESULT_PASS

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "checks": [
                {"name": check.name, "result": check.result, "detail": check.detail}
                for check in self.checks
            ],
        }


def vet_contribution_record(
    record: dict[str, Any],
    *,
    catalog: DeviceCatalog | None = None,
) -> VetReport:
    """Run all deterministic checks against one contribution record."""

    resolved_catalog = catalog if catalog is not None else load_device_catalog()
    record = record if isinstance(record, dict) else {}
    fingerprint = record.get("fingerprint") if isinstance(record.get("fingerprint"), dict) else {}

    checks: list[VetCheck] = [
        _check_record_version(record),
        _check_no_identifiers(record),
        _check_fingerprint(fingerprint),
        _check_layout_known(fingerprint, resolved_catalog),
        _check_catalog_collision(fingerprint, resolved_catalog),
        _check_coverage_consistency(record),
        _check_label_evidence(record),
    ]
    return VetReport(checks=tuple(checks))


def _check_record_version(record: dict[str, Any]) -> VetCheck:
    version = record.get("record_version")
    if version == RECORD_VERSION:
        return VetCheck("record_version", RESULT_PASS)
    return VetCheck(
        "record_version", RESULT_FAIL, f"unknown record_version {version!r} (expected {RECORD_VERSION})"
    )


def _check_no_identifiers(record: dict[str, Any]) -> VetCheck:
    if record_contains_identifier(record):
        return VetCheck("no_identifiers", RESULT_FAIL, "record still carries a device/account identifier")
    return VetCheck("no_identifiers", RESULT_PASS)


def _check_fingerprint(fingerprint: dict[str, Any]) -> VetCheck:
    layout_code = fingerprint.get("layout_code")
    model_code = fingerprint.get("model_code")
    if layout_code is None or model_code is None:
        return VetCheck("fingerprint", RESULT_FAIL, "missing layout_code/model_code")
    if layout_code == 0 and model_code == 0:
        return VetCheck(
            "fingerprint", RESULT_FAIL, "all-zero identity (inverter link was down during capture)"
        )
    return VetCheck("fingerprint", RESULT_PASS)


def _check_layout_known(fingerprint: dict[str, Any], catalog: DeviceCatalog) -> VetCheck:
    layout_code = fingerprint.get("layout_code")
    for layout in catalog.layouts:
        if layout_code in layout.layout_codes:
            return VetCheck("layout_known", RESULT_PASS, f"layout {layout.key}")
    return VetCheck(
        "layout_known", RESULT_WARN, f"layout_code {layout_code!r} is new; needs a layout definition"
    )


def _check_catalog_collision(fingerprint: dict[str, Any], catalog: DeviceCatalog) -> VetCheck:
    layout_code = fingerprint.get("layout_code")
    model_code = fingerprint.get("model_code")
    for entry in catalog.devices:
        if entry.fingerprint.layout_code == layout_code and entry.fingerprint.model_code == model_code:
            return VetCheck(
                "catalog_collision",
                RESULT_WARN,
                f"already cataloged as {entry.entry_key!r}; treat as an update, not a new entry",
            )
    return VetCheck("catalog_collision", RESULT_PASS, "new device")


def _check_coverage_consistency(record: dict[str, Any]) -> VetCheck:
    coverage = _coverage_registers(record.get("register_coverage"))
    if not coverage:
        return VetCheck("coverage_consistency", RESULT_WARN, "no register coverage captured")
    proposed = record.get("proposed_schema")
    if not isinstance(proposed, dict):
        return VetCheck("coverage_consistency", RESULT_PASS, "no proposed schema to check")
    spec_registers = _proposed_spec_registers(proposed)
    out_of_coverage = sorted(spec_registers - coverage)
    if out_of_coverage:
        return VetCheck(
            "coverage_consistency",
            RESULT_WARN,
            f"proposed registers outside captured coverage: {out_of_coverage[:8]}",
        )
    return VetCheck("coverage_consistency", RESULT_PASS)


def _check_label_evidence(record: dict[str, Any]) -> VetCheck:
    evidence = record.get("label_evidence") if isinstance(record.get("label_evidence"), dict) else {}
    numeric = evidence.get("numeric") if isinstance(evidence.get("numeric"), dict) else {}
    enum = evidence.get("enum") if isinstance(evidence.get("enum"), dict) else {}
    unique = _count_unique(numeric) + _count_unique(enum)
    if unique == 0:
        return VetCheck(
            "label_evidence", RESULT_WARN, "no unique label↔register bindings; sensors would be unlabeled"
        )
    return VetCheck("label_evidence", RESULT_PASS, f"{unique} unique bindings")


def _coverage_registers(coverage: Any) -> set[int]:
    registers: set[int] = set()
    if not isinstance(coverage, list):
        return registers
    for block in coverage:
        if isinstance(block, (list, tuple)) and len(block) >= 2:
            try:
                start, count = int(block[0]), int(block[1])
            except (TypeError, ValueError):
                continue
            registers.update(range(start, start + count))
    return registers


def _proposed_spec_registers(proposed: dict[str, Any]) -> set[int]:
    registers: set[int] = set()
    spec_sets = proposed.get("spec_sets")
    if isinstance(spec_sets, dict):
        for specs in spec_sets.values():
            if not isinstance(specs, list):
                continue
            for spec in specs:
                if isinstance(spec, dict) and "register" in spec:
                    try:
                        registers.add(int(spec["register"]))
                    except (TypeError, ValueError):
                        continue
    return registers


def _count_unique(bindings: dict[str, Any]) -> int:
    items = bindings.get("bindings") if isinstance(bindings, dict) else None
    if not isinstance(items, list):
        return 0
    return sum(1 for item in items if isinstance(item, dict) and item.get("status") == "unique")
