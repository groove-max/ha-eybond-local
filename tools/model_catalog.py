#!/usr/bin/env python3
"""Commercial inverter model catalog tool and generated-journal renderer.

This is repository maintenance tooling. It owns commercial model identity,
lifecycle, validation state, limitations, and sanitized source summaries, and
cross-references the runtime inverter catalog to DERIVE protocol, fingerprint,
surface, driver, profile, schema, read-only state, and capability counts. The
runtime integration never consumes this catalog; the generated Markdown journal
is a view, never an input to detection.

Commands:
    model_catalog.py list
    model_catalog.py show <model-key>
    model_catalog.py validate
    model_catalog.py render [--output PATH] [--check] [--format markdown]

The bare ``--format markdown --output PATH [--check]`` form (no subcommand) is
an alias for ``render`` so the existing generated-docs quality-gate machinery
can drive it.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.metadata.device_catalog_loader import (  # noqa: E402
    load_device_catalog,
)
from custom_components.eybond_local.metadata.profile_loader import (  # noqa: E402
    load_driver_profile,
)
from custom_components.eybond_local.metadata.register_schema_loader import (  # noqa: E402
    load_register_schema,
)
from custom_components.eybond_local.support.matrix import (  # noqa: E402
    build_profile_support_matrix,
)

CATALOG_DIR = REPO_ROOT / "catalog" / "inverter_models"
GENERATED_DOC = REPO_ROOT / "docs" / "generated" / "INVERTER_MODEL_CATALOG.generated.md"

LIFECYCLES = ("research", "experimental", "supported", "deprecated")
HARDWARE_STATES = ("none", "reported", "captured", "confirmed")
TELEMETRY_STATES = ("unknown", "partial", "confirmed")
CONTROL_STATES = ("none", "partial", "confirmed")
SOURCE_KINDS = (
    "support_archive",
    "github_issue",
    "github_comment",
    "forum_post",
    "private_message",
    "documentation",
    "third_party_impl",
    "register_map",
    "maintainer_test",
)
VISIBILITIES = ("public", "private")
ASSERTIONS = (
    "commercial_identity",
    "fingerprint",
    "runtime_compatibility",
    "telemetry_validation",
    "write_validation",
    "control_behavior",
    "register_map",
    "hardware_test",
)

# A "confirmed" validation dimension should be backed by a source asserting the
# corresponding evidence (else the render flags it as a weaker/stronger claim).
_CONFIRMED_BACKING = {
    "hardware": {"hardware_test", "fingerprint", "telemetry_validation"},
    "telemetry": {"telemetry_validation"},
    "controls": {"write_validation"},
}

_KEY_RE = re.compile(r"^[a-z0-9_]+$")
_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_PN_SHAPED = re.compile(r"[A-Za-z][0-9]{13,}")
_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_FORBIDDEN_KEYS = {
    "serial",
    "serial_number",
    "sn",
    "password",
    "secret",
    "token",
    "collector_pn",
    "pn",
    "ip",
    "ip_address",
    "account",
    "ssid",
    "mac",
}


# --- Loading --------------------------------------------------------------


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_manifest(catalog_dir: Path = CATALOG_DIR) -> dict:
    return _load_json(catalog_dir / "catalog.json")


def load_models(catalog_dir: Path = CATALOG_DIR) -> list[dict]:
    """Return all model records, deterministically ordered by file name."""

    models_dir = catalog_dir / "models"
    return [_load_json(p) for p in sorted(models_dir.glob("*.json"))]


def load_sources(catalog_dir: Path = CATALOG_DIR) -> list[dict]:
    """Return all source records, deterministically ordered by file name."""

    sources_dir = catalog_dir / "sources"
    return [_load_json(p) for p in sorted(sources_dir.glob("*.json"))]


# --- Runtime cross-reference ----------------------------------------------


@dataclass(frozen=True, slots=True)
class ResolvedDescriptor:
    device_key: str
    found: bool
    model_name: str
    surface_key: str
    surface_found: bool
    protocol: str
    driver: str
    variant: str
    profile_name: str
    register_schema_name: str
    tier: str
    read_only: bool | None
    detection: str
    fingerprint: dict | None
    anchors: tuple
    capability_count: int
    validation_state_counts: dict
    support_tier_counts: dict
    measurement_count: int
    binary_sensor_count: int


def _detection_method(device) -> str:
    fp = getattr(device, "fingerprint", None)
    if fp is not None and (fp.layout_code or fp.model_code or fp.rated_power_one_of):
        return "fingerprint"
    if getattr(device, "anchors", ()):
        return "anchors"
    if getattr(device, "family_fallback", False):
        return "family"
    return "unspecified"


def resolve_descriptor(device_key: str, catalog=None) -> ResolvedDescriptor:
    """Resolve one runtime device descriptor key into derived support state."""

    if catalog is None:
        catalog = load_device_catalog()
    device = next((d for d in catalog.devices if d.entry_key == device_key), None)
    if device is None:
        return ResolvedDescriptor(
            device_key=device_key, found=False, model_name="", surface_key="",
            surface_found=False, protocol="", driver="", variant="", profile_name="",
            register_schema_name="", tier="", read_only=None, detection="",
            fingerprint=None, anchors=(), capability_count=0,
            validation_state_counts={}, support_tier_counts={},
            measurement_count=0, binary_sensor_count=0,
        )

    surface = catalog.surfaces.get(device.surface_key)
    profile_name = surface.binding.profile_name if surface else ""
    schema_name = surface.binding.register_schema_name if surface else ""

    capability_count = 0
    validation_state_counts: dict = {}
    support_tier_counts: dict = {}
    if profile_name:
        try:
            matrix = build_profile_support_matrix(load_driver_profile(profile_name))
            summary = matrix["summary"]
            capability_count = int(summary["capabilities"])
            validation_state_counts = dict(summary["validation_state_counts"])
            support_tier_counts = dict(summary["support_tier_counts"])
        except Exception:  # noqa: BLE001 - derivation is best-effort/diagnostic
            pass

    measurement_count = 0
    binary_sensor_count = 0
    if schema_name:
        try:
            schema = load_register_schema(schema_name)
            measurement_count = len(schema.measurement_descriptions)
            binary_sensor_count = len(schema.binary_sensor_descriptions)
        except Exception:  # noqa: BLE001
            pass

    fp = getattr(device, "fingerprint", None)
    fingerprint = None
    if fp is not None and (fp.layout_code or fp.model_code or fp.rated_power_one_of):
        fingerprint = {
            "layout_code": fp.layout_code,
            "model_code": fp.model_code,
            "rated_power_one_of": list(fp.rated_power_one_of),
        }

    return ResolvedDescriptor(
        device_key=device_key,
        found=True,
        model_name=device.model_name,
        surface_key=device.surface_key,
        surface_found=surface is not None,
        protocol=surface.protocol_key if surface else "",
        driver=surface.binding.driver_key if surface else "",
        variant=surface.binding.variant_key if surface else "",
        profile_name=profile_name,
        register_schema_name=schema_name,
        tier=surface.tier if surface else "",
        read_only=surface.read_only if surface else None,
        detection=_detection_method(device),
        fingerprint=fingerprint,
        anchors=tuple(device.anchors),
        capability_count=capability_count,
        validation_state_counts=validation_state_counts,
        support_tier_counts=support_tier_counts,
        measurement_count=measurement_count,
        binary_sensor_count=binary_sensor_count,
    )


def _primary_resolution(model: dict, catalog) -> ResolvedDescriptor | None:
    """Return the first resolvable descriptor of a model, for table rows."""

    for variant in model.get("variants", []):
        for key in variant.get("device_descriptor_keys", []):
            resolved = resolve_descriptor(key, catalog)
            if resolved.found:
                return resolved
    return None


# --- Schema validation (hand-rolled; jsonschema is not a dependency) ------


def _err(errors: list[str], ctx: str, message: str) -> None:
    errors.append(f"{ctx}: {message}")


def _check_str(obj: dict, key: str, ctx: str, errors: list[str], *, required=True) -> None:
    if key not in obj:
        if required:
            _err(errors, ctx, f"missing required field '{key}'")
        return
    if not isinstance(obj[key], str) or not obj[key]:
        _err(errors, ctx, f"field '{key}' must be a non-empty string")


# --- Schema-driven validation ---------------------------------------------
#
# A small JSON-Schema (draft-07 subset) interpreter so the checked-in
# `schemas/*.schema.json` files are the single source of truth for record shape
# (type, required, additionalProperties, enum, const, pattern, minLength,
# minItems, items, and `format: date`). jsonschema is intentionally not a
# project dependency.

_SCHEMAS_DIR = CATALOG_DIR / "schemas"


@lru_cache(maxsize=None)
def _load_schema(name: str) -> dict:
    return _load_json(_SCHEMAS_DIR / name)


def _type_ok(value, type_name: str) -> bool:
    if type_name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "boolean":
        return isinstance(value, bool)
    return isinstance(value, {"string": str, "array": list, "object": dict}[type_name])


def _is_valid_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _validate_against_schema(instance, schema: dict, ctx: str, errors: list[str]) -> None:
    type_name = schema.get("type")
    if type_name and not _type_ok(instance, type_name):
        _err(errors, ctx, f"expected type '{type_name}'")
        return
    if "const" in schema and instance != schema["const"]:
        _err(errors, ctx, f"must equal {schema['const']!r}")
    if "enum" in schema and instance not in schema["enum"]:
        _err(errors, ctx, f"must be one of {schema['enum']}")
    if isinstance(instance, str):
        if "minLength" in schema and len(instance) < schema["minLength"]:
            _err(errors, ctx, f"shorter than minLength {schema['minLength']}")
        pattern = schema.get("pattern")
        if pattern and not re.search(pattern, instance):
            _err(errors, ctx, f"does not match pattern {pattern}")
        if schema.get("format") == "date" and not _is_valid_date(instance):
            _err(errors, ctx, "is not a valid calendar date")
    if isinstance(instance, list):
        if "minItems" in schema and len(instance) < schema["minItems"]:
            _err(errors, ctx, f"has fewer than minItems {schema['minItems']}")
        item_schema = schema.get("items")
        if item_schema:
            for index, item in enumerate(instance):
                _validate_against_schema(item, item_schema, f"{ctx}[{index}]", errors)
    if isinstance(instance, dict):
        properties = schema.get("properties", {})
        for required in schema.get("required", []):
            if required not in instance:
                _err(errors, ctx, f"missing required field '{required}'")
        if schema.get("additionalProperties") is False:
            for key in instance:
                if key not in properties:
                    _err(errors, ctx, f"unknown field '{key}'")
        for key, sub_schema in properties.items():
            if key in instance:
                _validate_against_schema(instance[key], sub_schema, f"{ctx}.{key}", errors)


def _validate_model_schema(model: dict, ctx: str, errors: list[str]) -> None:
    _validate_against_schema(model, _load_schema("model.schema.json"), ctx, errors)


def _validate_source_schema(source: dict, ctx: str, errors: list[str]) -> None:
    _validate_against_schema(source, _load_schema("source.schema.json"), ctx, errors)


# --- Privacy scan ---------------------------------------------------------


def _scan_identifiers(value, ctx: str, errors: list[str]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str) and key.lower() in _FORBIDDEN_KEYS:
                _err(errors, ctx, f"forbidden personal-identifier field '{key}'")
            _scan_identifiers(item, f"{ctx}.{key}", errors)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _scan_identifiers(item, f"{ctx}[{index}]", errors)
    elif isinstance(value, str):
        if _PN_SHAPED.search(value):
            _err(errors, ctx, "value contains a PN-shaped identifier token")
        if _IPV4.search(value):
            _err(errors, ctx, "value contains an IP address")


# --- Full validation ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ValidationReport:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors


def _safe_load_records(directory: Path, label: str, errors: list[str]) -> list:
    """Load every *.json under a directory, reporting unreadable files as errors.

    Fail-closed: a missing/syntactically-invalid file becomes a validation error
    instead of an uncaught exception, so validate always returns a report.
    """

    records: list = []
    if not directory.exists():
        return records
    for path in sorted(directory.glob("*.json")):
        try:
            records.append(_load_json(path))
        except (json.JSONDecodeError, OSError) as exc:
            _err(errors, f"{label}:{path.name}", f"unreadable or invalid JSON ({type(exc).__name__})")
    return records


def validate_catalog(catalog_dir: Path = CATALOG_DIR, *, runtime_catalog=None) -> ValidationReport:
    errors: list[str] = []
    warnings: list[str] = []

    # Manifest: schema-driven and fail-closed (a non-object/null root or an
    # unreadable file yields errors, never a traceback). A `null` root parses to
    # None and is still validated against the schema (which rejects it).
    manifest_loaded = True
    manifest = None
    try:
        manifest = load_manifest(catalog_dir)
    except (json.JSONDecodeError, OSError) as exc:
        _err(errors, "catalog.json", f"unreadable or invalid JSON ({type(exc).__name__})")
        manifest_loaded = False
    if manifest_loaded:
        _validate_against_schema(manifest, _load_schema("catalog.schema.json"), "catalog.json", errors)

    models_raw = _safe_load_records(catalog_dir / "models", "model", errors)
    sources_raw = _safe_load_records(catalog_dir / "sources", "source", errors)

    # Schema + privacy per record. Only records that pass SCHEMA validation
    # (structurally well-formed objects) are handed to the semantic pass below,
    # so a malformed record yields errors instead of crashing the validator.
    valid_models: list[dict] = []
    for model in models_raw:
        if not isinstance(model, dict):
            _err(errors, "model:<non-object>", "record must be a JSON object")
            continue
        ctx = f"model:{model.get('model_key', '?')}"
        schema_before = len(errors)
        _validate_model_schema(model, ctx, errors)
        schema_clean = len(errors) == schema_before
        _scan_identifiers(model, ctx, errors)
        if schema_clean:
            valid_models.append(model)

    valid_sources: list[dict] = []
    for source in sources_raw:
        if not isinstance(source, dict):
            _err(errors, "source:<non-object>", "record must be a JSON object")
            continue
        ctx = f"source:{source.get('source_key', '?')}"
        schema_before = len(errors)
        _validate_source_schema(source, ctx, errors)
        schema_clean = len(errors) == schema_before
        _scan_identifiers(source, ctx, errors)
        if schema_clean:
            valid_sources.append(source)

    # Duplicate keys (across all object records carrying a string key).
    model_keys = [m.get("model_key") for m in models_raw if isinstance(m, dict)]
    source_keys = [s.get("source_key") for s in sources_raw if isinstance(s, dict)]
    for key in sorted({k for k in model_keys if isinstance(k, str) and model_keys.count(k) > 1}):
        _err(errors, "models", f"duplicate model_key '{key}'")
    for key in sorted({k for k in source_keys if isinstance(k, str) and source_keys.count(k) > 1}):
        _err(errors, "sources", f"duplicate source_key '{key}'")

    # Build the reference index from SCHEMA-CLEAN sources only: a model that
    # references a corrupted source must get an "unknown source_key" error rather
    # than having the validator consume the broken record.
    source_index = {
        s["source_key"]: s
        for s in valid_sources
        if isinstance(s.get("source_key"), str)
    }

    # Public source exposing a private reference.
    for source in valid_sources:
        if source.get("visibility") == "public" and str(source.get("reference", "")).startswith("private:"):
            _err(errors, f"source:{source.get('source_key')}", "public source exposes a private reference")

    if runtime_catalog is None:
        runtime_catalog = load_device_catalog()

    for model in valid_models:
        ctx = f"model:{model.get('model_key', '?')}"
        # Source reference integrity.
        for source_key in model.get("source_keys", []):
            if source_key not in source_index:
                _err(errors, ctx, f"unknown source_key '{source_key}'")
        # Runtime descriptor + surface integrity, per-variant surface compatibility.
        model_has_safe_surface = False
        model_has_writable_surface = False
        for vindex, variant in enumerate(model.get("variants", [])):
            vctx = f"{ctx}.variants[{vindex}]"
            # Descriptors inside one variant are different immutable fingerprints
            # of the SAME runtime behavior, so they must resolve to one surface.
            variant_surfaces: set[str] = set()
            for device_key in variant.get("device_descriptor_keys", []):
                resolved = resolve_descriptor(device_key, runtime_catalog)
                if not resolved.found:
                    _err(errors, vctx, f"runtime descriptor '{device_key}' does not exist")
                    continue
                if not resolved.surface_found:
                    _err(errors, vctx, f"descriptor '{device_key}' surface '{resolved.surface_key}' does not exist")
                    continue
                variant_surfaces.add(resolved.surface_key)
                model_has_safe_surface = True
                if resolved.read_only is False:
                    model_has_writable_surface = True
            if len(variant_surfaces) > 1:
                _err(errors, vctx, f"incompatible surfaces within one variant: {sorted(variant_surfaces)}")
        if model.get("lifecycle") == "supported" and not model_has_safe_surface:
            _err(errors, ctx, "supported model resolves to no valid runtime descriptor/surface")

        # Confirmed validation dimensions should be backed by a source assertion.
        model_assertions: set[str] = set()
        for source_key in model.get("source_keys", []):
            source = source_index.get(source_key)
            if source:
                model_assertions.update(source.get("assertions", []))
        validation = model.get("validation", {})
        for dimension, backing in _CONFIRMED_BACKING.items():
            if validation.get(dimension) == "confirmed" and not (model_assertions & backing):
                warnings.append(
                    f"{ctx}: validation.{dimension}=confirmed without a backing source assertion {sorted(backing)}"
                )
        # Research models with no source.
        if model.get("lifecycle") == "research" and not model.get("source_keys"):
            warnings.append(f"{ctx}: research model has no source")

        # Coverage cross-check: runtime_control_surface must not overstate what
        # the resolved runtime surface can actually expose. `available` claims
        # the base runtime surface itself is writable, so it contradicts an
        # all-read-only resolution. (`device_scoped_overlay`/`hardware_confirmed`
        # controls can legitimately come from a learned overlay on a read-only
        # base, so they are not tied to the base read-only flag.)
        coverage = model.get("coverage")
        control_surface = (
            coverage.get("runtime_control_surface") if isinstance(coverage, dict) else None
        )
        if control_surface == "available" and not model_has_writable_surface:
            _err(
                errors,
                ctx,
                "coverage.runtime_control_surface='available' but every resolved runtime "
                "surface is read-only",
            )
        elif control_surface in ("none", "read_only") and model_has_writable_surface:
            warnings.append(
                f"{ctx}: coverage.runtime_control_surface='{control_surface}' but a resolved "
                "runtime surface is writable"
            )

    # Aliases shared across manufacturers.
    alias_owners: dict[str, set[str]] = {}
    for model in valid_models:
        for alias in [model.get("model"), *model.get("aliases", [])]:
            if alias:
                alias_owners.setdefault(alias.lower(), set()).add(model.get("manufacturer", ""))
    for alias, manufacturers in sorted(alias_owners.items()):
        if len(manufacturers) > 1:
            warnings.append(f"alias '{alias}' shared across manufacturers {sorted(manufacturers)}")

    # Runtime descriptors with no commercial model record are NOT integrity
    # problems: per the plan they are normal family-level runtime coverage and
    # are rendered in their own journal section (see family_level_descriptors).

    return ValidationReport(errors=tuple(errors), warnings=tuple(sorted(set(warnings))))


def family_level_descriptors(catalog_dir: Path = CATALOG_DIR, *, runtime_catalog=None) -> list[ResolvedDescriptor]:
    """Return runtime descriptors not claimed by any commercial model record.

    These are generic protocol families (PI30/PI18/PI41/SmartESS compatibility)
    and SMG family fallbacks: expected family-level runtime coverage, not gaps.
    """

    if runtime_catalog is None:
        runtime_catalog = load_device_catalog()
    referenced: set[str] = set()
    for model in load_models(catalog_dir):
        if not isinstance(model, dict):
            continue
        for variant in model.get("variants", []):
            if isinstance(variant, dict):
                referenced.update(variant.get("device_descriptor_keys", []))
    return [
        resolve_descriptor(device.entry_key, runtime_catalog)
        for device in runtime_catalog.devices
        if device.entry_key not in referenced
    ]


# --- Rendering ------------------------------------------------------------


def _sorted_models(models: list[dict]) -> list[dict]:
    return sorted(
        models,
        key=lambda m: (
            str(m.get("manufacturer", "")).lower(),
            str(m.get("model", "")).lower(),
            str(m.get("model_key", "")),
        ),
    )


def _variant_resolution(variant: dict, catalog) -> ResolvedDescriptor | None:
    for device_key in variant.get("device_descriptor_keys", []):
        resolved = resolve_descriptor(device_key, catalog)
        if resolved.found:
            return resolved
    return None


def _is_fully_supported(model: dict, catalog) -> bool:
    """Return whether a model belongs in the Supported (not Limited) table.

    A supported model with unconfirmed controls, partial telemetry, or a
    read-only runtime surface is presented under Limited Or Experimental, so the
    journal never overstates a model's support level.
    """

    if model.get("lifecycle") != "supported":
        return False
    validation = model.get("validation", {})
    if validation.get("controls") != "confirmed" or validation.get("telemetry") != "confirmed":
        return False
    for variant in model.get("variants", []):
        resolved = _variant_resolution(variant, catalog)
        if resolved is None or resolved.read_only:
            return False
    return bool(model.get("variants"))


def _table_rows(model: dict, catalog) -> list[str]:
    """Return one table row per variant so multi-variant models are not hidden."""

    validation = model.get("validation", {})
    variants = model.get("variants", [])
    multi = len(variants) > 1
    rows: list[str] = []
    for variant in variants:
        resolved = _variant_resolution(variant, catalog)
        protocol = resolved.protocol if resolved else "?"
        detection = resolved.detection if resolved else "?"
        tier = resolved.tier if resolved else "?"
        label = model.get("model", "")
        if multi:
            label = f"{label} — {variant.get('label', variant.get('variant_key'))}"
        rows.append(
            f"| {model.get('manufacturer', '')} | {label} | {protocol} "
            f"| {detection} | {tier} | {validation.get('telemetry', '?')} "
            f"| {validation.get('controls', '?')} | {validation.get('hardware', '?')} |"
        )
    return rows


_TABLE_HEADER = (
    "| Manufacturer | Model | Protocol | Detection | Runtime Tier | Telemetry | Controls | Hardware |\n"
    "|---|---|---|---|---|---|---|---|"
)


def _fingerprint_text(resolved: ResolvedDescriptor) -> str:
    if resolved.fingerprint:
        fp = resolved.fingerprint
        rated = ", ".join(str(p) for p in fp["rated_power_one_of"]) or "—"
        return f"layout {fp['layout_code']}, model {fp['model_code']}, rated {rated}"
    if resolved.anchors:
        return "; ".join(
            f"{a.get('key')}={a.get('equals', a.get('one_of', ''))}" for a in resolved.anchors
        )
    return "—"


def _counts_text(resolved: ResolvedDescriptor) -> str:
    if not resolved.profile_name:
        return "no model-specific profile"
    vs = ", ".join(f"{k} {v}" for k, v in sorted(resolved.validation_state_counts.items())) or "—"
    st = ", ".join(f"{k} {v}" for k, v in sorted(resolved.support_tier_counts.items())) or "—"
    return f"{resolved.capability_count} ({vs}); support tiers: {st}"


def _coverage_text(coverage: dict) -> str:
    """Render the three independent coverage dimensions in one compact line."""

    if not isinstance(coverage, dict):
        return "runtime ?, cloud ?, vendor map ?"
    return (
        f"runtime {coverage.get('runtime_control_surface', '?')}, "
        f"cloud {coverage.get('smartess_control_surface', '?')}, "
        f"vendor map {coverage.get('vendor_register_map', '?')}"
    )


def _render_model_detail(model: dict, catalog, sources_index: dict) -> list[str]:
    lines: list[str] = []
    title = f"{model.get('manufacturer', '')} — {model.get('model', '')} (`{model.get('model_key', '')}`)"
    lines.append(f"### {title}")
    lines.append("")
    lines.append(f"- Lifecycle: {model.get('lifecycle', '')}")
    aliases = model.get("aliases", [])
    lines.append(f"- Aliases: {', '.join(aliases) if aliases else '—'}")
    validation = model.get("validation", {})
    lines.append(
        f"- Validation: hardware {validation.get('hardware', '?')}, "
        f"telemetry {validation.get('telemetry', '?')}, controls {validation.get('controls', '?')}"
    )
    coverage = model.get("coverage", {})
    lines.append(f"- Coverage: {_coverage_text(coverage)}")
    coverage_notes = coverage.get("notes", []) if isinstance(coverage, dict) else []
    if coverage_notes:
        lines.append("  - Coverage notes:")
        for note in coverage_notes:
            lines.append(f"    - {note}")
    lines.append(f"- Summary: {model.get('knowledge_summary', '')}")
    lines.append("- Variants:")
    for variant in model.get("variants", []):
        lines.append(f"  - `{variant.get('variant_key')}` — {variant.get('label')}")
        descriptors = variant.get("device_descriptor_keys", [])
        lines.append(f"    - Descriptors: {', '.join(descriptors)}")
        firmware = variant.get("known_firmware", [])
        lines.append(f"    - Known firmware: {', '.join(firmware) if firmware else '—'}")
        for device_key in descriptors:
            resolved = resolve_descriptor(device_key, catalog)
            if not resolved.found:
                lines.append(f"    - `{device_key}`: MISSING runtime descriptor")
                continue
            ro = "yes" if resolved.read_only else "no"
            lines.append(
                f"    - `{device_key}` → surface `{resolved.surface_key}` "
                f"(driver {resolved.driver}, variant {resolved.variant})"
            )
            lines.append(
                f"      - Protocol: {resolved.protocol} | Detection: {resolved.detection} "
                f"({_fingerprint_text(resolved)})"
            )
            lines.append(
                f"      - Tier: {resolved.tier} | Read-only: {ro} "
                f"| Profile: {resolved.profile_name or '—'} | Schema: {resolved.register_schema_name or '—'}"
            )
            lines.append(
                f"      - Capabilities: {_counts_text(resolved)} "
                f"| Telemetry: {resolved.measurement_count} measurements, "
                f"{resolved.binary_sensor_count} binary sensors"
            )
    limitations = model.get("known_limitations", [])
    if limitations:
        lines.append("- Known limitations:")
        for limitation in limitations:
            lines.append(f"  - {limitation}")
    else:
        lines.append("- Known limitations: —")

    # Evidence: source count only.
    #
    # Keep raw references (URLs, private archive ids, external project links)
    # inside source records for maintainers.  The generated catalog is public
    # user-facing documentation, so it must not publish source references,
    # source titles, or source summaries even when the source itself is marked
    # public.
    source_keys = model.get("source_keys", [])
    lines.append(f"- Evidence: {len(source_keys)} source(s)")
    lines.append("")
    return lines


def render_markdown(catalog_dir: Path = CATALOG_DIR, *, runtime_catalog=None) -> str:
    manifest = load_manifest(catalog_dir)
    models = load_models(catalog_dir)
    sources = load_sources(catalog_dir)
    sources_index = {s.get("source_key"): s for s in sources}
    if runtime_catalog is None:
        runtime_catalog = load_device_catalog()

    ordered = _sorted_models(models)
    variant_count = sum(len(m.get("variants", [])) for m in models)

    lines: list[str] = []
    lines.append("# Inverter Model Catalog")
    lines.append("")
    lines.append("Generated by `tools/model_catalog.py`. Do not edit by hand.")
    lines.append("")
    lines.append(f"- Model catalog version: {manifest.get('catalog_version')}")
    lines.append(f"- Runtime inverter catalog version: {runtime_catalog.catalog_version}")
    lines.append(f"- Models: {len(models)} | Variants: {variant_count}")
    lines.append("")

    # Grouping is by derived support state, not lifecycle alone: a supported
    # model with unconfirmed controls / partial telemetry / a read-only surface
    # is presented under Limited so the journal never overstates support.
    research = [m for m in ordered if m.get("lifecycle") == "research"]
    supported = [
        m for m in ordered
        if m.get("lifecycle") != "research" and _is_fully_supported(m, runtime_catalog)
    ]
    limited = [
        m for m in ordered
        if m.get("lifecycle") != "research" and not _is_fully_supported(m, runtime_catalog)
    ]

    def _emit_table(models_group: list[dict], empty_text: str) -> None:
        if models_group:
            lines.append(_TABLE_HEADER)
            for model in models_group:
                lines.extend(_table_rows(model, runtime_catalog))
        else:
            lines.append(empty_text)
        lines.append("")

    lines.append("## Supported Models")
    lines.append("")
    _emit_table(supported, "None.")

    lines.append("## Limited Or Experimental Models")
    lines.append("")
    _emit_table(limited, "None.")

    lines.append("## Research Queue")
    lines.append("")
    _emit_table(research, "No known commercial models without a safe built-in runtime path.")

    lines.append("## Family-Level Runtime Coverage")
    lines.append("")
    lines.append(
        "Runtime descriptors with no specific commercial model record. These are "
        "generic protocol families and family fallbacks, reported as runtime "
        "coverage rather than gaps."
    )
    lines.append("")
    family = sorted(family_level_descriptors(catalog_dir, runtime_catalog=runtime_catalog),
                    key=lambda r: r.device_key)
    if family:
        lines.append("| Descriptor | Protocol | Surface | Tier | Read-only |")
        lines.append("|---|---|---|---|---|")
        for resolved in family:
            ro = "yes" if resolved.read_only else "no"
            lines.append(
                f"| `{resolved.device_key}` | {resolved.protocol} | {resolved.surface_key} "
                f"| {resolved.tier} | {ro} |"
            )
    else:
        lines.append("Every runtime descriptor maps to a commercial model record.")
    lines.append("")

    lines.append("## Model Details")
    lines.append("")
    for model in ordered:
        lines.extend(_render_model_detail(model, runtime_catalog, sources_index))

    lines.append("## Integrity Findings")
    lines.append("")
    report = validate_catalog(catalog_dir, runtime_catalog=runtime_catalog)
    if report.errors:
        lines.append("Errors:")
        for error in report.errors:
            lines.append(f"- {error}")
        lines.append("")
    if report.warnings:
        lines.append("Warnings:")
        for warning in report.warnings:
            lines.append(f"- {warning}")
        lines.append("")
    if not report.errors and not report.warnings:
        lines.append("No integrity findings.")
        lines.append("")

    return "\n".join(lines).rstrip("\n") + "\n"


# --- CLI ------------------------------------------------------------------


def _cmd_list(_argv: list[str]) -> int:
    catalog = load_device_catalog()
    for model in _sorted_models(load_models()):
        resolved = _primary_resolution(model, catalog)
        descriptor_count = sum(len(v.get("device_descriptor_keys", [])) for v in model.get("variants", []))
        validation = model.get("validation", {})
        surface = resolved.surface_key if resolved else "—"
        print(
            f"{model.get('model_key'):32} {model.get('lifecycle'):12} "
            f"descriptors={descriptor_count} surface={surface} "
            f"hw={validation.get('hardware')} tel={validation.get('telemetry')} ctl={validation.get('controls')}"
        )
    return 0


def _cmd_show(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="model_catalog.py show")
    parser.add_argument("model_key")
    args = parser.parse_args(argv)

    models = {m.get("model_key"): m for m in load_models()}
    model = models.get(args.model_key)
    if model is None:
        print(f"unknown model_key: {args.model_key}", file=sys.stderr)
        return 1
    catalog = load_device_catalog()
    sources_index = {s.get("source_key"): s for s in load_sources()}
    print("\n".join(_render_model_detail(model, catalog, sources_index)).rstrip())
    report = validate_catalog(runtime_catalog=catalog)
    model_errors = [e for e in report.errors if f":{args.model_key}" in e]
    model_warnings = [w for w in report.warnings if f":{args.model_key}" in w]
    if model_errors or model_warnings:
        print("\nValidation:")
        for line in model_errors:
            print(f"  ERROR {line}")
        for line in model_warnings:
            print(f"  WARN  {line}")
    return 0


def _cmd_validate(_argv: list[str]) -> int:
    report = validate_catalog()
    for warning in report.warnings:
        print(f"WARN  {warning}")
    for error in report.errors:
        print(f"ERROR {error}")
    if report.errors:
        print(f"\nFAILED: {len(report.errors)} error(s), {len(report.warnings)} warning(s)")
        return 1
    print(f"OK: 0 errors, {len(report.warnings)} warning(s)")
    return 0


def _cmd_render(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="model_catalog.py render")
    parser.add_argument("--format", choices=("markdown",), default="markdown")
    parser.add_argument("--output", help="output file path; prints to stdout when omitted")
    parser.add_argument("--check", action="store_true", help="check --output matches current content")
    args = parser.parse_args(argv)

    if args.check and not args.output:
        parser.error("--check requires --output")

    rendered = render_markdown()
    expected = rendered if rendered.endswith("\n") else rendered + "\n"

    if args.output:
        output_path = Path(args.output).expanduser()
        if args.check:
            if not output_path.exists():
                print(f"missing:{output_path}")
                return 1
            if output_path.read_text(encoding="utf-8") != expected:
                print(f"out_of_sync:{output_path}")
                return 1
            print(f"in_sync:{output_path}")
            return 0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(expected, encoding="utf-8")
        print(output_path)
        return 0

    sys.stdout.write(expected)
    return 0


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    sub = raw[0] if raw and not raw[0].startswith("-") else None
    rest = raw[1:] if sub else raw
    if sub == "list":
        return _cmd_list(rest)
    if sub == "show":
        return _cmd_show(rest)
    if sub == "validate":
        return _cmd_validate(rest)
    if sub in ("render", None):
        return _cmd_render(rest)
    print(f"unknown command: {sub}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
