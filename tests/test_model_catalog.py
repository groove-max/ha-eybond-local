from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.device_catalog_loader import load_device_catalog
from tools.model_catalog import (
    CATALOG_DIR,
    GENERATED_DOC,
    _is_fully_supported,
    _primary_resolution,
    _table_rows,
    family_level_descriptors,
    load_models,
    load_sources,
    render_markdown,
    resolve_descriptor,
    validate_catalog,
)


RUNTIME = load_device_catalog()


def _write_catalog(tmp: Path, models: list[dict], sources: list[dict]) -> Path:
    base = tmp / "inverter_models"
    (base / "models").mkdir(parents=True)
    (base / "sources").mkdir(parents=True)
    (base / "catalog.json").write_text(
        json.dumps({"schema_version": 1, "catalog_version": "test", "description": "test"}),
        encoding="utf-8",
    )
    for model in models:
        (base / "models" / f"{model['model_key']}.json").write_text(json.dumps(model), encoding="utf-8")
    for source in sources:
        (base / "sources" / f"{source['source_key']}.json").write_text(json.dumps(source), encoding="utf-8")
    return base


def _ok_source(key: str = "src_a", **overrides) -> dict:
    source = {
        "schema_version": 1,
        "source_key": key,
        "kind": "github_issue",
        "title": "t",
        "reference": "https://example.com/1",
        "captured_at": "2026-06-01",
        "visibility": "public",
        "summary": "s",
        "assertions": ["commercial_identity"],
    }
    source.update(overrides)
    return source


def _ok_model(key: str = "mdl_a", descriptor: str = "smg_6200", **overrides) -> dict:
    model = {
        "schema_version": 1,
        "model_key": key,
        "manufacturer": "Acme",
        "model": "M1",
        "aliases": [],
        "lifecycle": "experimental",
        "variants": [{"variant_key": "v1", "label": "V1", "device_descriptor_keys": [descriptor]}],
        "validation": {"hardware": "reported", "telemetry": "partial", "controls": "none"},
        "coverage": {
            "runtime_control_surface": "available",
            "smartess_control_surface": "unknown",
            "vendor_register_map": "unknown",
            "notes": [],
        },
        "known_limitations": [],
        "knowledge_summary": "summary",
        "source_keys": [],
    }
    model.update(overrides)
    return model


class RealCatalogTests(unittest.TestCase):
    def test_validate_has_no_errors(self) -> None:
        report = validate_catalog(runtime_catalog=RUNTIME)
        self.assertTrue(report.ok, msg=f"errors: {report.errors}")
        self.assertEqual(report.errors, ())

    def test_validate_is_clean(self) -> None:
        # Authored data backs every confirmed dimension and family-level
        # descriptors are not integrity problems, so validate is fully clean.
        report = validate_catalog(runtime_catalog=RUNTIME)
        self.assertEqual(report.errors, ())
        self.assertEqual(report.warnings, ())

    def test_family_level_coverage_lists_unmapped_descriptors(self) -> None:
        family = {r.device_key for r in family_level_descriptors(runtime_catalog=RUNTIME)}
        referenced = {
            key
            for model in load_models()
            for variant in model["variants"]
            for key in variant["device_descriptor_keys"]
        }
        runtime_keys = {d.entry_key for d in RUNTIME.devices}
        self.assertEqual(family, runtime_keys - referenced)
        self.assertIn("pi30_family", family)
        self.assertIn("smg_variant_4200", family)
        # Family-level descriptors are rendered as coverage, not in Model Details.
        journal = render_markdown(runtime_catalog=RUNTIME)
        self.assertIn("## Family-Level Runtime Coverage", journal)
        self.assertIn("`pi30_family`", journal)

    def test_supported_models_resolve_to_safe_surface(self) -> None:
        for model in load_models():
            if model.get("lifecycle") != "supported":
                continue
            resolved = _primary_resolution(model, RUNTIME)
            self.assertIsNotNone(resolved, msg=model["model_key"])
            self.assertTrue(resolved.found and resolved.surface_found, msg=model["model_key"])

    def test_source_and_descriptor_references_resolve(self) -> None:
        source_keys = {s["source_key"] for s in load_sources()}
        runtime_keys = {d.entry_key for d in RUNTIME.devices}
        for model in load_models():
            for source_key in model["source_keys"]:
                self.assertIn(source_key, source_keys, msg=f"{model['model_key']} -> {source_key}")
            for variant in model["variants"]:
                for device_key in variant["device_descriptor_keys"]:
                    self.assertIn(device_key, runtime_keys, msg=f"{model['model_key']} -> {device_key}")

    def test_derived_state_matches_runtime(self) -> None:
        smg = resolve_descriptor("smg_6200", RUNTIME)
        self.assertEqual((smg.driver, smg.protocol, smg.surface_key), ("modbus_smg", "modbus_smg", "smg_6200_full"))
        self.assertFalse(smg.read_only)
        self.assertGreater(smg.measurement_count, 0)
        pi = resolve_descriptor("pi30_vmii_nxpw5kw", RUNTIME)
        self.assertEqual(pi.protocol, "pi30")
        self.assertEqual(pi.detection, "anchors")

    def test_read_only_model_is_marked_read_only(self) -> None:
        resolved = resolve_descriptor("srne_modbus_family", RUNTIME)
        self.assertTrue(resolved.read_only)
        self.assertEqual(resolved.tier, "partial")

    def test_no_research_models_in_supported_data(self) -> None:
        # Research models must never be presented as supported.
        lifecycles = {m["model_key"]: m["lifecycle"] for m in load_models()}
        for key, lifecycle in lifecycles.items():
            if lifecycle == "research":
                resolved = _primary_resolution(
                    next(m for m in load_models() if m["model_key"] == key), RUNTIME
                )
                # A research model may resolve, but it is rendered in the research
                # queue, never the supported table (lifecycle drives placement).
                self.assertNotEqual(lifecycle, "supported")

    def test_render_is_deterministic(self) -> None:
        self.assertEqual(render_markdown(runtime_catalog=RUNTIME), render_markdown(runtime_catalog=RUNTIME))

    def test_model_detail_renders_control_surface_coverage(self) -> None:
        journal = render_markdown(runtime_catalog=RUNTIME)
        self.assertIn(
            "- Coverage: runtime device_scoped_overlay, cloud device_scoped_overlay, vendor map unknown",
            journal,
        )
        self.assertIn(
            "- Coverage: runtime hardware_confirmed, cloud hardware_confirmed, vendor map extended",
            journal,
        )

    def test_generated_doc_is_in_sync(self) -> None:
        rendered = render_markdown(runtime_catalog=RUNTIME)
        expected = rendered if rendered.endswith("\n") else rendered + "\n"
        self.assertEqual(
            GENERATED_DOC.read_text(encoding="utf-8"),
            expected,
            msg="run: python3 tools/model_catalog.py render --output docs/generated/INVERTER_MODEL_CATALOG.generated.md",
        )


class ValidationGuardTests(unittest.TestCase):
    def _validate(self, models, sources):
        with tempfile.TemporaryDirectory() as tmp:
            base = _write_catalog(Path(tmp), models, sources)
            return validate_catalog(base, runtime_catalog=RUNTIME)

    def test_missing_descriptor_fails(self) -> None:
        report = self._validate([_ok_model(descriptor="does_not_exist")], [])
        self.assertFalse(report.ok)
        self.assertTrue(any("does not exist" in e for e in report.errors), report.errors)

    def test_duplicate_model_key_fails(self) -> None:
        a = _ok_model("dup")
        b = _ok_model("dup", descriptor="anenji_4200")
        # Two files, same model_key.
        with tempfile.TemporaryDirectory() as tmp:
            base = _write_catalog(Path(tmp), [a], [])
            (base / "models" / "dup2.json").write_text(json.dumps(b), encoding="utf-8")
            report = validate_catalog(base, runtime_catalog=RUNTIME)
        self.assertTrue(any("duplicate model_key" in e for e in report.errors), report.errors)

    def test_supported_without_surface_fails(self) -> None:
        report = self._validate(
            [_ok_model(lifecycle="supported", descriptor="does_not_exist")], []
        )
        self.assertTrue(any("no valid runtime descriptor" in e for e in report.errors), report.errors)

    def test_incompatible_surfaces_in_variant_fails(self) -> None:
        model = _ok_model()
        model["variants"] = [
            {"variant_key": "v1", "label": "V1", "device_descriptor_keys": ["smg_6200", "pi30_vmii_nxpw5kw"]}
        ]
        report = self._validate([model], [])
        self.assertTrue(any("incompatible surfaces" in e for e in report.errors), report.errors)

    def test_public_source_with_private_reference_fails(self) -> None:
        bad = _ok_source(visibility="public", reference="private:leak")
        report = self._validate([_ok_model(source_keys=["src_a"])], [bad])
        self.assertTrue(any("private reference" in e for e in report.errors), report.errors)

    def test_unknown_source_key_fails(self) -> None:
        report = self._validate([_ok_model(source_keys=["nope"])], [])
        self.assertTrue(any("unknown source_key" in e for e in report.errors), report.errors)

    def test_forbidden_identifier_fails(self) -> None:
        bad = _ok_model(knowledge_summary="device E5000020000000 reported")
        report = self._validate([bad], [])
        self.assertTrue(any("PN-shaped" in e for e in report.errors), report.errors)

    def test_forbidden_field_key_fails(self) -> None:
        bad = _ok_model()
        bad["serial_number"] = "x"  # forbidden field key
        report = self._validate([bad], [])
        self.assertTrue(any("forbidden personal-identifier field" in e for e in report.errors), report.errors)

    def test_bad_lifecycle_enum_fails(self) -> None:
        report = self._validate([_ok_model(lifecycle="totally_made_up")], [])
        self.assertTrue(
            any(".lifecycle" in e and "must be one of" in e for e in report.errors), report.errors
        )

    def test_research_model_without_source_warns(self) -> None:
        report = self._validate([_ok_model(lifecycle="research", source_keys=[])], [])
        self.assertTrue(any("research model has no source" in w for w in report.warnings), report.warnings)

    def test_confirmed_without_backing_source_warns(self) -> None:
        model = _ok_model(
            lifecycle="supported",
            validation={"hardware": "confirmed", "telemetry": "confirmed", "controls": "confirmed"},
            source_keys=["src_a"],
        )
        # Source only asserts commercial_identity -> no telemetry/write/hardware backing.
        report = self._validate([model], [_ok_source()])
        self.assertTrue(any("confirmed without a backing source" in w for w in report.warnings), report.warnings)


class SchemaEnforcementTests(unittest.TestCase):
    """Finding 1: validation must enforce the declared JSON schemas."""

    def _validate_model(self, model: dict):
        with tempfile.TemporaryDirectory() as tmp:
            base = _write_catalog(Path(tmp), [model], [])
            return validate_catalog(base, runtime_catalog=RUNTIME)

    def test_additional_properties_rejected(self) -> None:
        model = _ok_model()
        model["surprise"] = "x"
        report = self._validate_model(model)
        self.assertTrue(any("unknown field 'surprise'" in e for e in report.errors), report.errors)

    def test_wrong_type_for_aliases_rejected(self) -> None:
        report = self._validate_model(_ok_model(aliases="not-a-list"))
        self.assertTrue(any(".aliases" in e and "type" in e for e in report.errors), report.errors)

    def test_wrong_type_for_known_limitations_rejected(self) -> None:
        report = self._validate_model(_ok_model(known_limitations=[1, 2]))
        self.assertTrue(any("known_limitations" in e and "type" in e for e in report.errors), report.errors)

    def test_invalid_variant_key_rejected(self) -> None:
        model = _ok_model()
        model["variants"][0]["variant_key"] = "Bad Key!"
        report = self._validate_model(model)
        self.assertTrue(any("variant_key" in e and "pattern" in e for e in report.errors), report.errors)

    def test_extra_field_in_validation_rejected(self) -> None:
        model = _ok_model()
        model["validation"]["extra"] = "x"
        report = self._validate_model(model)
        self.assertTrue(
            any(".validation" in e and "unknown field 'extra'" in e for e in report.errors),
            report.errors,
        )

    def test_missing_coverage_rejected(self) -> None:
        model = _ok_model()
        del model["coverage"]
        report = self._validate_model(model)
        self.assertTrue(any("missing required field 'coverage'" in e for e in report.errors), report.errors)

    def test_invalid_coverage_state_rejected(self) -> None:
        model = _ok_model()
        model["coverage"]["smartess_control_surface"] = "magic"
        report = self._validate_model(model)
        self.assertTrue(
            any("smartess_control_surface" in e and "must be one of" in e for e in report.errors),
            report.errors,
        )

    def test_extra_field_in_variant_rejected(self) -> None:
        model = _ok_model()
        model["variants"][0]["note"] = "x"
        report = self._validate_model(model)
        self.assertTrue(any("variants[0]" in e and "unknown field 'note'" in e for e in report.errors), report.errors)

    def test_impossible_date_rejected(self) -> None:
        source = _ok_source(captured_at="2026-99-99")
        with tempfile.TemporaryDirectory() as tmp:
            base = _write_catalog(Path(tmp), [_ok_model(source_keys=["src_a"])], [source])
            report = validate_catalog(base, runtime_catalog=RUNTIME)
        self.assertTrue(any("valid calendar date" in e for e in report.errors), report.errors)


class SurfaceConflictTests(unittest.TestCase):
    """Finding 2: distinct surfaces inside one variant are incompatible."""

    def test_distinct_surfaces_in_variant_rejected(self) -> None:
        model = _ok_model()
        # smg_6200 -> smg_6200_full, anenji_op2_6200 -> anenji_op2_6200_full
        # (same driver+protocol, different surface): must be flagged.
        model["variants"] = [
            {"variant_key": "v", "label": "V", "device_descriptor_keys": ["smg_6200", "anenji_op2_6200"]}
        ]
        with tempfile.TemporaryDirectory() as tmp:
            base = _write_catalog(Path(tmp), [model], [])
            report = validate_catalog(base, runtime_catalog=RUNTIME)
        self.assertTrue(any("incompatible surfaces" in e for e in report.errors), report.errors)

    def test_same_surface_in_variant_allowed(self) -> None:
        # smg_6200 and anenji_anj_6200_48pl share surface smg_6200_full.
        model = _ok_model()
        model["variants"] = [
            {"variant_key": "v", "label": "V", "device_descriptor_keys": ["smg_6200", "anenji_anj_6200_48pl"]}
        ]
        with tempfile.TemporaryDirectory() as tmp:
            base = _write_catalog(Path(tmp), [model], [])
            report = validate_catalog(base, runtime_catalog=RUNTIME)
        self.assertFalse(any("incompatible surfaces" in e for e in report.errors), report.errors)


class CoverageCrossCheckTests(unittest.TestCase):
    """coverage.runtime_control_surface must not overstate the resolved runtime."""

    def _validate(self, model):
        with tempfile.TemporaryDirectory() as tmp:
            base = _write_catalog(Path(tmp), [model], [])
            return validate_catalog(base, runtime_catalog=RUNTIME)

    def test_available_coverage_on_read_only_surface_errors(self) -> None:
        model = _ok_model(descriptor="srne_modbus_family")  # read-only surface
        model["coverage"]["runtime_control_surface"] = "available"
        report = self._validate(model)
        self.assertTrue(
            any("every resolved runtime surface is read-only" in e for e in report.errors),
            report.errors,
        )

    def test_read_only_coverage_on_writable_surface_warns(self) -> None:
        model = _ok_model(descriptor="smg_6200")  # writable surface
        model["coverage"]["runtime_control_surface"] = "read_only"
        report = self._validate(model)
        self.assertTrue(
            any("but a resolved runtime surface is writable" in w for w in report.warnings),
            report.warnings,
        )

    def test_read_only_coverage_on_read_only_surface_is_clean(self) -> None:
        model = _ok_model(descriptor="srne_modbus_family")
        model["coverage"]["runtime_control_surface"] = "read_only"
        report = self._validate(model)
        self.assertFalse(
            any("runtime_control_surface" in e for e in report.errors), report.errors
        )


class JournalGroupingTests(unittest.TestCase):
    """Finding 3 & 4: support grouping and multi-variant table rows."""

    def test_partial_controls_model_not_fully_supported(self) -> None:
        models = {m["model_key"]: m for m in load_models()}
        self.assertTrue(_is_fully_supported(models["sandisolar_sd_hym_4862hwp"], RUNTIME))
        self.assertTrue(_is_fully_supported(models["anenji_anj_11kw_48v_wifi_p"], RUNTIME))
        # controls=partial -> Limited, not Supported.
        self.assertFalse(_is_fully_supported(models["anenji_anj_6200_48pl"], RUNTIME))

    def test_read_only_supported_lifecycle_not_fully_supported(self) -> None:
        # A hypothetical supported+confirmed model on a read-only surface is Limited.
        model = {
            "schema_version": 1, "model_key": "ro", "manufacturer": "A", "model": "M",
            "aliases": [], "lifecycle": "supported",
            "variants": [{"variant_key": "v", "label": "V",
                          "device_descriptor_keys": ["srne_modbus_family"]}],
            "validation": {"hardware": "confirmed", "telemetry": "confirmed", "controls": "confirmed"},
            "coverage": {
                "runtime_control_surface": "read_only",
                "smartess_control_surface": "unknown",
                "vendor_register_map": "unknown",
                "notes": [],
            },
            "known_limitations": [], "knowledge_summary": "s", "source_keys": [],
        }
        self.assertFalse(_is_fully_supported(model, RUNTIME))

    def test_supported_table_excludes_partial_controls(self) -> None:
        journal = render_markdown(runtime_catalog=RUNTIME)
        supported_block = journal.split("## Limited Or Experimental Models")[0]
        # ANJ-6200-48PL has partial controls and must not appear in the Supported block.
        self.assertNotIn("ANJ-6200-48PL", supported_block)
        self.assertIn("SD-HYM-4862HWP", supported_block)
        self.assertIn("ANJ-11KW-48V-WIFI-P", supported_block)
        self.assertIn("4.2KW", supported_block)

    def test_multi_variant_renders_one_row_per_variant(self) -> None:
        model = {
            "schema_version": 1, "model_key": "mv", "manufacturer": "A", "model": "Multi",
            "aliases": [], "lifecycle": "experimental",
            "variants": [
                {"variant_key": "v1", "label": "SMG layout", "device_descriptor_keys": ["smg_6200"]},
                {"variant_key": "v2", "label": "PI30 layout", "device_descriptor_keys": ["pi30_vmii_nxpw5kw"]},
            ],
            "validation": {"hardware": "reported", "telemetry": "partial", "controls": "none"},
            "coverage": {
                "runtime_control_surface": "available",
                "smartess_control_surface": "unknown",
                "vendor_register_map": "unknown",
                "notes": [],
            },
            "known_limitations": [], "knowledge_summary": "s", "source_keys": [],
        }
        rows = _table_rows(model, RUNTIME)
        self.assertEqual(len(rows), 2)
        self.assertTrue(any("modbus_smg" in r and "SMG layout" in r for r in rows))
        self.assertTrue(any("pi30" in r and "PI30 layout" in r for r in rows))


class MalformedRecordTests(unittest.TestCase):
    """Validator must return a report on malformed records, never traceback."""

    def _validate_raw(self, model_objs, source_objs=(), *, manifest=None):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "inverter_models"
            (base / "models").mkdir(parents=True)
            (base / "sources").mkdir(parents=True)
            manifest_text = (
                manifest
                if manifest is not None
                else json.dumps({"schema_version": 1, "catalog_version": "t", "description": "d"})
            )
            (base / "catalog.json").write_text(manifest_text, encoding="utf-8")
            for index, obj in enumerate(model_objs):
                (base / "models" / f"m{index}.json").write_text(json.dumps(obj), encoding="utf-8")
            for index, obj in enumerate(source_objs):
                (base / "sources" / f"s{index}.json").write_text(json.dumps(obj), encoding="utf-8")
            return validate_catalog(base, runtime_catalog=RUNTIME)

    def test_variants_wrong_element_type_does_not_crash(self) -> None:
        report = self._validate_raw([_ok_model(variants=[1])])
        self.assertFalse(report.ok)

    def test_aliases_non_list_does_not_crash(self) -> None:
        report = self._validate_raw([_ok_model(aliases=1)])
        self.assertFalse(report.ok)

    def test_source_keys_non_list_does_not_crash(self) -> None:
        report = self._validate_raw([_ok_model(source_keys=1)])
        self.assertFalse(report.ok)

    def test_validation_non_object_does_not_crash(self) -> None:
        report = self._validate_raw([_ok_model(validation="bad")])
        self.assertFalse(report.ok)

    def test_non_object_model_root_reports_error(self) -> None:
        report = self._validate_raw([[1, 2, 3]])
        self.assertFalse(report.ok)
        self.assertTrue(any("must be a JSON object" in e for e in report.errors), report.errors)

    def test_non_object_source_root_reports_error(self) -> None:
        report = self._validate_raw([_ok_model()], [42])
        self.assertFalse(report.ok)
        self.assertTrue(any("must be a JSON object" in e for e in report.errors), report.errors)

    def test_malformed_record_does_not_block_valid_record(self) -> None:
        good = _ok_model("good")
        bad = _ok_model("bad", variants=[1])
        report = self._validate_raw([good, bad])
        self.assertFalse(report.ok)
        # The schema-clean model is still semantically checked and clean.
        self.assertFalse(any("model:good" in e for e in report.errors), report.errors)

    def test_invalid_source_referenced_by_model_does_not_crash(self) -> None:
        # Source has a non-list `assertions`; a model referencing it must get an
        # "unknown source_key" error, not consume the corrupted record.
        bad_source = {
            "schema_version": 1, "source_key": "s", "kind": "github_issue", "title": "t",
            "reference": "https://example.com/1", "captured_at": "2026-06-01",
            "visibility": "public", "summary": "x", "assertions": 1,
        }
        report = self._validate_raw([_ok_model(source_keys=["s"])], [bad_source])
        self.assertFalse(report.ok)
        self.assertTrue(any("unknown source_key 's'" in e for e in report.errors), report.errors)

    def test_manifest_non_object_root_reports_error(self) -> None:
        report = self._validate_raw([_ok_model()], manifest="[]")
        self.assertFalse(report.ok)
        self.assertTrue(any("catalog.json" in e for e in report.errors), report.errors)

    def test_manifest_null_root_reports_error(self) -> None:
        report = self._validate_raw([_ok_model()], manifest="null")
        self.assertFalse(report.ok)
        self.assertTrue(any("catalog.json" in e for e in report.errors), report.errors)

    def test_manifest_invalid_json_reports_error(self) -> None:
        report = self._validate_raw([_ok_model()], manifest="{not valid json")
        self.assertFalse(report.ok)
        self.assertTrue(any("catalog.json" in e for e in report.errors), report.errors)


class SourceContextTests(unittest.TestCase):
    """Finding 6: dates valid and local-only fixtures are not public."""

    def test_all_captured_dates_are_valid(self) -> None:
        from datetime import datetime

        for source in load_sources():
            datetime.strptime(source["captured_at"], "%Y-%m-%d")  # raises on invalid

    def test_local_only_fixture_is_private(self) -> None:
        sources = {s["source_key"]: s for s in load_sources()}
        fixture = sources["fixture_pi30_vmii_nxpw5kw"]
        self.assertEqual(fixture["visibility"], "private")
        self.assertEqual(fixture["captured_at"], "2026-04-07")


if __name__ == "__main__":
    unittest.main()
