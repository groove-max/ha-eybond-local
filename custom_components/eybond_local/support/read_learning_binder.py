"""Bind cloud-labeled sensor values to local modbus registers by value correlation.

During a learning session the cloud polls the shadow proxy, which answers every
read from the synthetic SEED register bank. The cloud's labeled "last data"
(``querySPDeviceLastData``) is therefore rendered FROM the very register values
we hold — alignment between a labeled cloud value and the raw registers is
structural, not temporal. That makes exact value correlation sound for every
numeric quantity, including otherwise-volatile ones (currents, power): both
sides describe the same frozen snapshot.

What correlation alone cannot resolve:
- zero/degenerate values (too many registers read 0) — recorded as skipped;
- several registers legitimately holding the same value (e.g. output voltage,
  inverter voltage and the rating register all 230.0 V) — recorded as
  ambiguous WITH the candidate list, never guessed;
- enum labels (the cloud sends a resolved string, not an int) — deferred to
  the read-enum learner, recorded as ``enum_label``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
import re
from typing import Any

from ..metadata.semantic_titles_loader import resolve_semantic_title
from ..payload.modbus import to_signed_16


_SCALES = (1, 10, 100, 1000)

METHOD_VALUE_CORRELATION = "value_correlation"

BIND_STATUS_UNIQUE = "unique"
BIND_STATUS_AMBIGUOUS = "ambiguous"
BIND_STATUS_NO_MATCH = "no_match"
BIND_STATUS_SKIPPED_ZERO = "skipped_zero"
BIND_STATUS_ENUM_LABEL = "enum_label"
BIND_STATUS_NOT_NUMERIC = "not_numeric"


_SMARTESS_FIELD_ID_RE = re.compile(
    r"(?:^|_)eybond_(?P<kind>read|ctrl)_(?P<ordinal>[1-9][0-9]*)$"
)


@dataclass(frozen=True, slots=True)
class ObservedControlEnumEvidence:
    """One same-session cloud-control enum causally bound to one register.

    This is stronger than a title hint: every value/label pair comes from a
    cloud control attempt whose exact local write was observed during the same
    shadow session. The provider field ordinal lets a read field reuse that
    evidence only when the provider identifies both surfaces as the same field.
    """

    provider_id: str
    session_id: str
    semantic_key: str
    cloud_field_id: str
    enum_table: str
    provider_field_ordinal: int
    register: int
    devcode: int
    devaddr: int
    value_labels: tuple[tuple[int, str], ...]

    def __post_init__(self) -> None:
        for value in (
            self.provider_id,
            self.session_id,
            self.semantic_key,
            self.cloud_field_id,
            self.enum_table,
        ):
            if type(value) is not str:
                raise TypeError("control_enum_evidence_string_invalid")
            if not value or value != value.strip():
                raise ValueError("control_enum_evidence_string_invalid")
        for value in (
            self.provider_field_ordinal,
            self.register,
            self.devcode,
            self.devaddr,
        ):
            if type(value) is not int:
                raise TypeError("control_enum_evidence_integer_invalid")
            if value <= 0:
                raise ValueError("control_enum_evidence_integer_invalid")
        if type(self.value_labels) is not tuple or len(self.value_labels) < 2:
            raise ValueError("control_enum_evidence_values_invalid")
        seen_values: set[int] = set()
        for pair in self.value_labels:
            if type(pair) is not tuple or len(pair) != 2:
                raise TypeError("control_enum_evidence_value_invalid")
            raw_value, label = pair
            if type(raw_value) is not int or type(label) is not str:
                raise TypeError("control_enum_evidence_value_invalid")
            if not label or label != label.strip() or raw_value in seen_values:
                raise ValueError("control_enum_evidence_value_invalid")
            seen_values.add(raw_value)


@dataclass(frozen=True, slots=True)
class ReadBindingCandidate:
    """One register that can render the labeled cloud value."""

    register: int
    divisor: int
    raw_value: int
    signed: bool

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "register": self.register,
            "divisor": self.divisor,
            "raw_value": self.raw_value,
            "signed": self.signed,
        }


@dataclass(frozen=True, slots=True)
class ReadLabelBinding:
    """The correlation verdict for one labeled cloud sensor."""

    cloud_id: str
    title: str
    unit: str
    cloud_value: str
    status: str
    candidates: tuple[ReadBindingCandidate, ...] = ()
    decimals: int = 0
    method: str = METHOD_VALUE_CORRELATION
    value_source: str = "seed_bank"

    @property
    def register(self) -> int | None:
        if self.status == BIND_STATUS_UNIQUE and self.candidates:
            return self.candidates[0].register
        return None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "cloud_id": self.cloud_id,
            "title": self.title,
            "unit": self.unit,
            "cloud_value": self.cloud_value,
            "status": self.status,
            "candidates": [candidate.to_json_dict() for candidate in self.candidates],
            "decimals": self.decimals,
            "method": self.method,
            "value_source": self.value_source,
        }


@dataclass(frozen=True, slots=True)
class ReadBindingReport:
    """All correlation verdicts for one labeled sensor list."""

    bindings: tuple[ReadLabelBinding, ...] = ()
    register_count: int = 0
    sensor_count: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def unique_bindings(self) -> tuple[ReadLabelBinding, ...]:
        return tuple(b for b in self.bindings if b.status == BIND_STATUS_UNIQUE)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "bindings": [binding.to_json_dict() for binding in self.bindings],
            "register_count": self.register_count,
            "sensor_count": self.sensor_count,
            "unique_count": len(self.unique_bindings),
            "notes": list(self.notes),
        }


def bind_cloud_labels_to_registers(
    *,
    sensors: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    registers: dict[Any, Any],
) -> ReadBindingReport:
    """Correlate labeled cloud sensors against a register→samples map.

    ``sensors``: items shaped like the cloud ``device_detail`` entries
    (``{"id", "par", "val", "unit"}``). ``registers``: the session read-map
    ``registers`` dict (``{"205": [2305], ...}`` — keys may be str or int).
    """

    register_values = _normalize_registers(registers)
    bindings: list[ReadLabelBinding] = []

    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        cloud_id = str(sensor.get("id") or "")
        title = str(sensor.get("par") or sensor.get("name") or "").strip()
        unit = str(sensor.get("unit") or "").strip()
        raw_value = str(sensor.get("val") if sensor.get("val") is not None else "").strip()
        if not title:
            continue

        try:
            target = float(raw_value)
        except (TypeError, ValueError):
            target = None
        if target is not None and not math.isfinite(target):
            # 'nan'/'inf'/'-inf' parse as floats but cannot reconstruct a raw
            # register word and would crash round() in _match_candidates; treat
            # them like a non-numeric value instead of failing the whole run.
            target = None
        if target is None:
            status = (
                BIND_STATUS_ENUM_LABEL
                if raw_value and not unit
                else BIND_STATUS_NOT_NUMERIC
            )
            bindings.append(
                ReadLabelBinding(
                    cloud_id=cloud_id,
                    title=title,
                    unit=unit,
                    cloud_value=raw_value,
                    status=status,
                )
            )
            continue

        if target == 0:
            bindings.append(
                ReadLabelBinding(
                    cloud_id=cloud_id,
                    title=title,
                    unit=unit,
                    cloud_value=raw_value,
                    status=BIND_STATUS_SKIPPED_ZERO,
                )
            )
            continue

        candidates = _match_candidates(target, register_values)
        decimals = _decimals_from_text(raw_value)
        if len(candidates) == 1:
            status = BIND_STATUS_UNIQUE
        elif candidates:
            status = BIND_STATUS_AMBIGUOUS
        else:
            status = BIND_STATUS_NO_MATCH
        bindings.append(
            ReadLabelBinding(
                cloud_id=cloud_id,
                title=title,
                unit=unit,
                cloud_value=raw_value,
                status=status,
                candidates=tuple(candidates),
                decimals=decimals,
            )
        )

    return ReadBindingReport(
        bindings=tuple(bindings),
        register_count=len(register_values),
        sensor_count=len(bindings),
    )


def _normalize_registers(registers: dict[Any, Any]) -> dict[int, tuple[int, ...]]:
    normalized: dict[int, tuple[int, ...]] = {}
    if not isinstance(registers, dict):
        return normalized
    for key, samples in registers.items():
        try:
            register = int(key)
        except (TypeError, ValueError):
            continue
        if isinstance(samples, (list, tuple)):
            values = tuple(int(value) for value in samples if isinstance(value, int))
        elif isinstance(samples, int):
            values = (samples,)
        else:
            continue
        if values:
            normalized[register] = values
    return normalized


def _match_candidates(
    target: float,
    register_values: dict[int, tuple[int, ...]],
) -> list[ReadBindingCandidate]:
    """Exact-match candidates, smallest divisor preferred per register."""

    best_per_register: dict[int, ReadBindingCandidate] = {}
    for divisor in _SCALES:
        scaled = target * divisor
        rounded = round(scaled)
        # The cloud renders values from these exact raw words: only exact
        # reconstructions count (a half-LSB tolerance covers float formatting).
        if abs(scaled - rounded) > 1e-6:
            continue
        for register, samples in register_values.items():
            if register in best_per_register:
                continue
            for raw in samples:
                if raw == rounded and rounded >= 0:
                    best_per_register[register] = ReadBindingCandidate(
                        register=register,
                        divisor=divisor,
                        raw_value=raw,
                        signed=False,
                    )
                    break
                if to_signed_16(raw) == rounded and rounded < 0:
                    best_per_register[register] = ReadBindingCandidate(
                        register=register,
                        divisor=divisor,
                        raw_value=raw,
                        signed=True,
                    )
                    break
    return sorted(best_per_register.values(), key=lambda item: (item.divisor, item.register))


def _decimals_from_text(value: str) -> int:
    text = str(value or "")
    if "." not in text:
        return 0
    return len(text.rsplit(".", 1)[1].strip())


# ---------------------------------------------------------------------------
# Read-enum matching (PR 3.3)
#
# The cloud sends RESOLVED enum strings ("Off-Grid Mode"), never the raw int,
# and a single session holds one frozen snapshot — so an enum table cannot be
# learned from one session alone. What CAN be done soundly now:
#  * invert the KNOWN enum tables of the source schema: find tables containing
#    a label that matches the cloud string, then registers currently holding
#    the mapped int — same unique/ambiguous discipline as numeric binding;
#  * record every (cloud_id, title, label) observation into the manifest so
#    repeated sessions in different device states accumulate table evidence.
# ---------------------------------------------------------------------------

ENUM_STATUS_UNIQUE = "unique"
ENUM_STATUS_AMBIGUOUS = "ambiguous"
ENUM_STATUS_NO_TABLE_MATCH = "no_table_match"


def normalize_enum_label(text: str) -> str:
    """Normalize one enum label for cross-vocabulary comparison."""

    return "".join(char for char in str(text or "").lower() if char.isalnum())


def _smartess_field_ordinal(field_id: object, *, kind: str) -> int | None:
    """Return one exact SmartESS read/control field ordinal, without coercion."""

    if type(field_id) is not str or field_id != field_id.strip():
        return None
    match = _SMARTESS_FIELD_ID_RE.search(field_id)
    if match is None or match.group("kind") != kind:
        return None
    return int(match.group("ordinal"))


def _labels_match(cloud_label: str, table_label: str) -> str:
    """Return the match kind ("exact"/"contains"/"") for two normalized labels."""

    if not cloud_label or not table_label:
        return ""
    if cloud_label == table_label:
        return "exact"
    # Generic binary labels are not evidence for a longer state. Without this
    # guard "Off-Grid Mode" matched every unrelated enum table containing an
    # "Off" row and made a known operating-mode register look ambiguous.
    if cloud_label in {"on", "off"} or table_label in {"on", "off"}:
        return ""
    if cloud_label in table_label or table_label in cloud_label:
        return "contains"
    return ""


def match_enum_bindings(
    *,
    read_bindings: dict[str, Any] | None,
    registers: dict[Any, Any],
    enum_tables: dict[str, Any] | None,
    register_enum_tables: dict[int, str] | None = None,
    control_enum_evidence: tuple[ObservedControlEnumEvidence, ...] = (),
    session_id: str = "",
) -> dict[str, Any]:
    """Match enum labels against known tables and their authoritative registers.

    ``register_enum_tables`` is optional for standalone/corpus callers. The
    overlay generator always supplies it from the effective schema so a raw
    value shared by unrelated registers cannot borrow the wrong enum table.
    """

    register_values = _normalize_registers(registers)
    bindings = []
    if isinstance(read_bindings, dict):
        bindings = [
            item
            for item in read_bindings.get("bindings", [])
            if isinstance(item, dict) and item.get("status") == BIND_STATUS_ENUM_LABEL
        ]
    if not bindings or not register_values or not isinstance(enum_tables, dict):
        return {"bindings": [], "unique_count": 0}

    trusted_control_evidence = (
        control_enum_evidence
        if type(control_enum_evidence) is tuple
        and type(session_id) is str
        and session_id
        and session_id == session_id.strip()
        else ()
    )

    results: list[dict[str, Any]] = []
    for item in bindings:
        cloud_label = normalize_enum_label(str(item.get("cloud_value") or ""))
        candidates: list[dict[str, Any]] = []
        for table_name, table in enum_tables.items():
            if not isinstance(table, dict):
                continue
            for raw_key, table_label in table.items():
                match_kind = _labels_match(cloud_label, normalize_enum_label(str(table_label)))
                if not match_kind:
                    continue
                try:
                    expected = int(raw_key)
                except (TypeError, ValueError):
                    continue
                for register, samples in register_values.items():
                    if register_enum_tables is not None and (
                        register_enum_tables.get(register) != str(table_name)
                    ):
                        continue
                    if expected in samples:
                        candidates.append(
                            {
                                "register": register,
                                "raw_value": expected,
                                "enum_table": str(table_name),
                                "table_label": str(table_label),
                                "match_kind": match_kind,
                            }
                        )
        read_semantic = resolve_semantic_title(str(item.get("title") or ""))
        read_ordinal = _smartess_field_ordinal(item.get("cloud_id"), kind="read")
        if read_semantic is not None and read_ordinal is not None:
            for evidence in trusted_control_evidence:
                if type(evidence) is not ObservedControlEnumEvidence:
                    continue
                if (
                    evidence.provider_id != "smartess"
                    or evidence.session_id != session_id
                    or evidence.semantic_key != read_semantic.semantic_key
                    or evidence.provider_field_ordinal != read_ordinal
                ):
                    continue
                table_name = (
                    register_enum_tables.get(evidence.register)
                    if isinstance(register_enum_tables, dict)
                    else None
                )
                table = enum_tables.get(table_name) if table_name else None
                if (
                    not isinstance(table, dict)
                    or evidence.enum_table != table_name
                ):
                    continue
                samples = register_values.get(evidence.register, ())
                for raw_value, control_label in evidence.value_labels:
                    if normalize_enum_label(control_label) != cloud_label:
                        continue
                    table_key: object
                    if raw_value in table:
                        table_key = raw_value
                    elif str(raw_value) in table:
                        table_key = str(raw_value)
                    else:
                        continue
                    if raw_value not in samples:
                        continue
                    candidates.append(
                        {
                            "register": evidence.register,
                            "raw_value": raw_value,
                            "enum_table": str(table_name),
                            "table_label": str(table[table_key]),
                            "cloud_control_label": control_label,
                            "cloud_control_field_id": evidence.cloud_field_id,
                            "match_kind": "control_exact",
                            "evidence_method": "same_session_control_enum",
                        }
                    )
        # Prefer exact label matches; containment only fills in when nothing
        # exact exists (keeps "Off-Grid Mode" from also matching "Grid Mode").
        exact = [
            candidate
            for candidate in candidates
            if candidate["match_kind"] in {"exact", "control_exact"}
        ]
        effective = exact if exact else candidates
        deduplicated: dict[tuple[int, int, str], dict[str, Any]] = {}
        for candidate in effective:
            key = (
                int(candidate["register"]),
                int(candidate["raw_value"]),
                str(candidate["enum_table"]),
            )
            existing = deduplicated.get(key)
            if existing is None or candidate["match_kind"] == "control_exact":
                deduplicated[key] = candidate
        effective = list(deduplicated.values())
        distinct_registers = sorted({candidate["register"] for candidate in effective})
        if len(distinct_registers) == 1:
            status = ENUM_STATUS_UNIQUE
        elif distinct_registers:
            status = ENUM_STATUS_AMBIGUOUS
        else:
            status = ENUM_STATUS_NO_TABLE_MATCH
        results.append(
            {
                "cloud_id": str(item.get("cloud_id") or ""),
                "title": str(item.get("title") or ""),
                "cloud_value": str(item.get("cloud_value") or ""),
                "status": status,
                "candidates": effective,
                "method": (
                    "same_session_control_enum"
                    if any(
                        candidate.get("evidence_method")
                        == "same_session_control_enum"
                        for candidate in effective
                    )
                    else "enum_table_inversion"
                ),
                "value_source": (
                    "seed_bank_and_observed_control"
                    if any(
                        candidate.get("evidence_method")
                        == "same_session_control_enum"
                        for candidate in effective
                    )
                    else "seed_bank"
                ),
            }
        )

    return {
        "bindings": results,
        "unique_count": sum(1 for item in results if item["status"] == ENUM_STATUS_UNIQUE),
    }
