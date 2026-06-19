# Inverter Model Catalog And Journal Plan

## Purpose

Add a maintainable catalog of commercial inverter models and generate a
human-readable support journal from it.

The model catalog answers:

- which commercial models are known to the project;
- which hardware or firmware variants are covered;
- how each variant maps to runtime detection descriptors and surfaces;
- which parts of support are implemented and hardware-validated;
- which limitations are currently known;
- which sanitized sources support the maintained conclusions.

The journal is a generated view of this catalog. It is not an input to runtime
detection and is never edited directly.

## Design Boundaries

### Runtime Catalog

`custom_components/eybond_local/protocol_catalogs/inverter_catalog.json`
continues to own:

- protocol probe actions;
- immutable and structural detection evidence;
- device and family descriptors;
- runtime surfaces;
- profile and register-schema selection;
- support-capture policy.

Runtime behavior must not depend on generated Markdown.

### Model Catalog

The new model catalog owns:

- manufacturer and commercial model names;
- aliases and branding;
- commercial hardware and firmware variants;
- links from those variants to runtime device descriptors;
- lifecycle and hardware-validation state;
- known limitations and maintainer summaries;
- sanitized references to supporting sources.

It does not duplicate protocol, fingerprint, surface, profile, schema, or
read-only policy when those values can be resolved from the runtime catalog.

### Source Records

Source records preserve the minimum context required to understand why catalog
information was added. A source may represent:

- a support archive;
- a GitHub issue or comment;
- a public forum post;
- a private forum or direct message;
- official documentation;
- a third-party implementation;
- a full or partial register map;
- a maintainer hardware test.

Raw private conversations, credentials, serial numbers, collector identifiers,
IP addresses, and account identifiers must not be committed. Private sources
use an opaque local reference and a sanitized technical summary.

### Research Workflow

Case or investigation management is explicitly outside this MVP. An agent may
keep temporary research notes outside the public catalog, but the built-in
catalog stores only maintained model state, source summaries, limitations, and
validation conclusions.

## Proposed Source Layout

```text
catalog/
  inverter_models/
    catalog.json
    models/
      <model-key>.json
    sources/
      <source-key>.json
    schemas/
      model.schema.json
      source.schema.json

docs/generated/
  INVERTER_MODEL_CATALOG.generated.md

tools/
  model_catalog.py
```

This catalog is repository maintenance data. It stays outside the Home
Assistant integration package in the MVP because runtime does not consume it.
That avoids adding a new runtime failure path while the administrative format
is still evolving.

If runtime later needs commercial manufacturer, model, alias, or support-state
metadata, the tool can compile a small validated JSON index into
`custom_components/eybond_local/protocol_catalogs/` without making the
maintenance source directly runtime-critical.

## Catalog Manifest

`catalog/inverter_models/catalog.json`:

```json
{
  "schema_version": 1,
  "catalog_version": "2026.06.1",
  "description": "Commercial inverter model support catalog"
}
```

The model catalog version is independent from the runtime inverter catalog
version. The generated journal displays both versions.

## Model Record

One file represents one commercial model, not one runtime descriptor:

```json
{
  "schema_version": 1,
  "model_key": "anenji_anj_6200_48pl",
  "manufacturer": "Anenji",
  "model": "ANJ-6200-48PL",
  "aliases": [],
  "lifecycle": "supported",
  "variants": [
    {
      "variant_key": "layout2_model8960",
      "label": "Known SMG layout 2 variant",
      "device_descriptor_keys": [
        "anenji_anj_6200_48pl"
      ],
      "known_firmware": [
        "2300_B0240802p1"
      ]
    }
  ],
  "validation": {
    "hardware": "confirmed",
    "telemetry": "confirmed",
    "controls": "partial"
  },
  "known_limitations": [
    "Not every writable setting has independent hardware confirmation."
  ],
  "knowledge_summary": "SMG layout 2 model using the SMG 6200 runtime surface.",
  "source_keys": [
    "github_issue_8",
    "support_archive_anenji_anj_6200_48pl_20260615"
  ]
}
```

### Stable Keys

`model_key` is a stable technical identifier. It is not renamed when display
capitalization, branding, or aliases change.

Recommended form:

```text
<manufacturer>_<commercial-model>
```

If the commercial identity is genuinely unknown, do not invent a public model
record from a runtime fingerprint alone. Such a fingerprint remains a runtime
descriptor until a commercial identity is established.

### Lifecycle

Allowed MVP lifecycle values:

- `research`: the model is known but no safe runtime support is available;
- `experimental`: a runtime path exists but support is incomplete or not
  sufficiently validated;
- `supported`: a safe built-in runtime path exists;
- `deprecated`: the record remains documented but is no longer recommended.

Lifecycle controls journal placement. It does not override runtime safety.

### Validation Dimensions

Validation is deliberately multidimensional:

- `hardware`: `none`, `reported`, `captured`, or `confirmed`;
- `telemetry`: `unknown`, `partial`, or `confirmed`;
- `controls`: `none`, `partial`, or `confirmed`.

`confirmed` requires a source record describing the relevant validation. A
model with confirmed reads but partial controls must not be presented as fully
validated.

### Variants

A model may link to multiple runtime descriptors for:

- hardware revisions;
- firmware-dependent layouts;
- protocol revisions;
- region-specific variants;
- multiple immutable fingerprints sharing one commercial name.

Conversely, several commercial models may link to the same descriptor or
surface when they are confirmed rebrands.

## Source Record

Example public source:

```json
{
  "schema_version": 1,
  "source_key": "github_issue_8",
  "kind": "github_issue",
  "title": "Anenji ANJ-6200-48PL user report",
  "reference": "https://github.com/groove-max/ha-eybond-local/issues/8",
  "captured_at": "2026-06-15",
  "visibility": "public",
  "summary": "Commercial model name and SUF output-priority behavior reported by the device owner.",
  "assertions": [
    "commercial_identity",
    "control_behavior"
  ]
}
```

Example private source:

```json
{
  "schema_version": 1,
  "source_key": "forum_pm_anenji_op2_20260612",
  "kind": "private_message",
  "title": "Anenji OP2 register information",
  "reference": "private:forum-pm-20260612-01",
  "captured_at": "2026-06-12",
  "visibility": "private",
  "summary": "Owner supplied OP2 live and configuration registers and confirmed the output2 enable write on hardware.",
  "assertions": [
    "register_map",
    "write_validation"
  ]
}
```

Example support archive source:

```json
{
  "schema_version": 1,
  "source_key": "support_archive_anenji_anj_6200_48pl_20260615",
  "kind": "support_archive",
  "title": "ANJ-6200-48PL support capture",
  "reference": "sha256:<archive-sha256>",
  "captured_at": "2026-06-15",
  "visibility": "private",
  "summary": "Complete SMG capture with layout 2, model code 8960, rated power 6200, and a working SMG 6200 runtime surface.",
  "assertions": [
    "fingerprint",
    "runtime_compatibility",
    "telemetry_validation"
  ]
}
```

The archive filename is optional. SHA-256 is the durable identity and allows a
local corpus scanner to find the original artifact without committing it.

## Register Maps And Protocol Documentation

The MVP source format can reference register maps, protocol documentation, and
third-party implementations, but it does not attempt to parse arbitrary PDF,
message, or source-code content during journal generation.

Journal generation consumes only maintained normalized summaries.

Future structured knowledge records may be added under:

```text
catalog/inverter_models/register_maps/
catalog/inverter_models/protocol_specs/
```

Model and source records already use stable keys, so adding those record types
does not require changing the model-journal architecture.

Importers may later extract draft register or protocol claims. Draft extraction
must remain a separate command and must never silently modify runtime schemas
or declare support.

## Derived Support State

The journal generator resolves each model variant through:

1. `device_descriptor_keys` in the model record;
2. matching `devices[]` in `inverter_catalog.json`;
3. each descriptor's `surface_key`;
4. the referenced surface's driver, variant, profile, schema, tier, and
   read-only policy;
5. profile capability metadata and validation annotations.

The following values are derived and must not be manually copied into model
records:

- protocol key;
- fingerprint and detection anchors;
- resolution class available from the descriptor;
- runtime surface;
- driver and variant;
- profile and register schema;
- read-only state;
- capability counts;
- profile validation-state counts;
- support-tier counts.

This keeps the journal synchronized with actual integration behavior.

## Generated Journal

`docs/generated/INVERTER_MODEL_CATALOG.generated.md` contains:

### Header

- model catalog version;
- runtime inverter catalog version;
- generation note;
- total model and variant counts.

### Supported Models

```text
| Manufacturer | Model | Protocol | Detection | Runtime Tier | Telemetry | Controls | Hardware |
```

### Limited Or Experimental Models

Includes:

- read-only model surfaces;
- family-compatible mappings;
- partial telemetry;
- unconfirmed controls;
- experimental runtime paths.

### Research Queue

Known commercial models without a safe built-in runtime path. These entries are
clearly separated from supported models.

### Model Details

Each model section includes:

- aliases;
- known variants and firmware;
- descriptor and surface links;
- derived fingerprint or anchors;
- profile and register-schema names;
- validation state;
- known limitations;
- sanitized evidence summary;
- source count and public references.

Private source references are not rendered. Their sanitized summaries may be
rendered only when explicitly marked safe for publication.

### Integrity Findings

The maintainer render reports:

- descriptors with no commercial model record;
- model records with missing descriptors;
- models resolving to conflicting surfaces;
- `supported` records with no safe runtime surface;
- confirmed validation states without corresponding sources;
- duplicate commercial aliases;
- stale generated output.

## Integration Runtime Interaction

### MVP

There is no new runtime dependency:

```text
model catalog --references--> runtime catalog
integration -----------------> runtime catalog, profiles, schemas
```

The model catalog cannot change detection or entity exposure. Its validator
reads runtime metadata to prove that journal claims are valid.

### Optional Follow-Up

After the format is stable, `model_catalog.py` may compile a runtime-safe index:

```text
custom_components/eybond_local/protocol_catalogs/inverter_model_index.json
```

That index would contain only:

- `model_key`;
- manufacturer;
- canonical model;
- aliases;
- descriptor-to-model mapping;
- public support label or limitations.

The integration could use it to improve device presentation after detection.
Detection must continue to work if this presentation layer is unavailable.

## Administrative Tool

`tools/model_catalog.py` provides:

```text
model_catalog.py list
model_catalog.py show <model-key>
model_catalog.py add-model
model_catalog.py add-source
model_catalog.py import-archive <zip>
model_catalog.py validate
model_catalog.py render
model_catalog.py render --check
```

### `list`

Shows model, lifecycle, descriptor count, derived surface, and validation
summary.

### `show`

Shows one self-contained administrative view:

- commercial identity;
- variants;
- runtime resolution;
- derived support state;
- limitations;
- source summaries;
- validation errors.

### `add-model`

Creates a schema-valid model skeleton. It does not modify the runtime catalog.

### `add-source`

Creates a sanitized source record and optionally attaches it to a model.

### `import-archive`

The first implementation:

- verifies that the input is a support archive;
- computes SHA-256;
- extracts sanitized catalog detection, fingerprint, profile, schema, surface,
  firmware, and capture date;
- searches for matching runtime descriptors and model variants;
- prints a proposed source record;
- writes only with an explicit confirmation flag.

It must not automatically mark a model as supported or confirmed.

### `validate`

Validation must fail for:

- invalid schema;
- duplicate keys;
- missing model/source references;
- missing runtime descriptors or surfaces;
- a supported model with no selectable safe surface;
- incompatible surfaces inside one variant;
- forbidden personal identifier fields;
- a public source exposing a private reference;
- generated journal drift when called from the quality gate.

Warnings cover:

- runtime descriptors without commercial model records;
- research models with no source;
- model aliases shared across manufacturers;
- validation claims weaker or stronger than available evidence.

### `render`

Renders the Markdown journal deterministically. `--check` compares generated
content without writing.

## Quality Gate Integration

Add the journal to the existing generated-document workflow:

```text
python3 tools/model_catalog.py validate
python3 tools/model_catalog.py render \
  --output docs/generated/INVERTER_MODEL_CATALOG.generated.md
python3 tools/model_catalog.py render \
  --check \
  --output docs/generated/INVERTER_MODEL_CATALOG.generated.md
```

`tools/quality_gate.py --refresh-generated` refreshes it. The normal quality
gate checks that it is current.

## Implementation Phases

### Phase 1: Read-Only Catalog Core

- Add manifest, model, and source schemas.
- Add loaders with deterministic iteration.
- Add runtime descriptor and surface cross-reference helpers.
- Implement `list`, `show`, and `validate`.
- Add unit tests for schema, references, privacy, and derived support state.

No integration runtime code changes.

### Phase 2: Journal Generator

- Implement model grouping by lifecycle and derived support state.
- Render summary tables and detailed model sections.
- Add checked-in generated Markdown.
- Add generated-doc freshness to the quality gate.
- Link the journal from `docs/README.md`.

### Phase 3: Initial Catalog Migration

Create commercial model records for descriptors whose identity is currently
known:

- SMG 6200;
- Anenji 4200 Protocol 1;
- Anenji ANJ-11KW-48V-WIFI-P;
- Anenji 6200 dual-output variant;
- Anenji 6200 single-output variant;
- Aninerel ANL-4200T-24L-W-PRO;
- Anenji ANJ-6200-48PL;
- PowMr VMII-NXPW5KW.

Do not create commercial models for generic PI30, PI18, PI41, SmartESS
compatibility, or SMG family fallbacks unless an actual commercial model is
known. The journal reports those as family-level runtime coverage.

Add sanitized source records from:

- the existing support archive corpus;
- linked GitHub issues;
- existing runtime catalog provenance;
- maintainer hardware knowledge.

Replace free-form runtime provenance strings with stable model/source references
only after the journal catalog is populated and validated. This replacement is
not required for the first journal render.

### Phase 4: Administrative Writes

- Implement `add-model` and `add-source`.
- Implement support-archive import in proposal mode.
- Add explicit write/apply mode.
- Add local corpus lookup by archive SHA-256.

### Phase 5: Structured Technical Knowledge

Only when needed:

- add partial and full register-map records;
- add protocol-spec records;
- add source-to-register claim provenance;
- compare known maps with built-in schemas;
- generate promotion suggestions without automatic runtime mutation.

### Phase 6: Optional Runtime Presentation Index

After the format has proven stable:

- compile public model presentation metadata;
- load it outside the detection-critical path;
- use it for manufacturer, model, aliases, and support presentation;
- retain runtime catalog descriptors as detection authority.

## Acceptance Criteria

- Every journal entry comes from a model record.
- Every supported model resolves to at least one valid runtime descriptor and
  safe surface.
- Runtime-derived data is not manually duplicated in model records.
- Research models are never presented as supported.
- Read-only and partially validated controls are visible in the journal.
- Private sources preserve sanitized technical context without exposing user
  identifiers or private references.
- The journal is deterministic and checked by the quality gate.
- Adding or editing journal data cannot change runtime detection in the MVP.
- Removing or renaming a runtime descriptor referenced by a model fails
  validation.
- A maintainer or agent can inspect one model through `show` and understand its
  identity, runtime mapping, validation state, limitations, and source basis
  without rereading the original conversation history.
