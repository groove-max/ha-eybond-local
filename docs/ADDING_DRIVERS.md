# Adding A New Driver / Profile

This project is designed to grow through transport-aware payload drivers plus declarative metadata, not through hardcoded model logic inside Home Assistant entities.

The preferred workflow is:

1. capture or import a local fixture
2. implement or extend driver detection
3. add read-side register schema
4. add declarative profile metadata
5. update the commercial model catalog when support state changes
6. preserve partial protocol knowledge in a durable, privacy-safe form
7. validate offline first
8. polish the Home Assistant UX only after the protocol path is stable

## Design Rule

Prefer the thinnest Python driver that can possibly work.

When deciding where new logic belongs, use this order:

1. `custom_components/eybond_local/protocol_catalogs/profiles/` for capability groups, writable metadata, conditions, presets, and support annotations
2. `custom_components/eybond_local/protocol_catalogs/register_schemas/` for read-side layouts, fields, enums, bit labels, and model overlays
3. `custom_components/eybond_local/payload/` for family-level framing and parse helpers
4. `custom_components/eybond_local/drivers/` only for probe, read, write, and procedural derived logic

Do not add user-facing metadata to a Python driver if the same information can live in JSON.

## Metadata Ownership Rule

When imported SmartESS assets, runtime compatibility overlays, and dedicated
local SmartESS runtime profiles all exist, keep their ownership explicit.

- Raw imported SmartESS asset truth belongs under `custom_components/eybond_local/protocol_catalogs/profiles/smartess_local/models/` and `custom_components/eybond_local/protocol_catalogs/register_schemas/smartess_local/models/`.
- Effective compatibility overlays stay under the runtime family that consumes
  them, for example `pi30_ascii/models/smartess_0925_compat.json`.
- Dedicated local SmartESS runtime profiles stay under `smartess_local/` and
  should not be treated as commercial model names.
- Do not blur SmartESS asset ids, raw inverter model strings, and commercial
  model names. Store commercial support conclusions in `catalog/inverter_models/`.

## Key Project Paths

Core integration code:

- `custom_components/eybond_local/collector/`
- `custom_components/eybond_local/payload/`
- `custom_components/eybond_local/drivers/`
- `custom_components/eybond_local/protocol_catalogs/inverter_catalog.json`
- `custom_components/eybond_local/protocol_catalogs/profiles/`
- `custom_components/eybond_local/protocol_catalogs/register_schemas/`
- `catalog/inverter_models/`
- `custom_components/eybond_local/runtime/`
- `custom_components/eybond_local/canonical_telemetry.py`
- `custom_components/eybond_local/config_flow.py`
- `custom_components/eybond_local/schema.py`

Offline and maintenance tooling:

- `tools/replay_fixture.py`
- `tools/import_fixture.py`
- `tools/validate_fixture_catalog.py`
- `tools/validate_profiles.py`
- `tools/quality_gate.py`

Fixtures and tests:

- `.local/fixtures/catalog/`
- `tests/`

## Recommended Workflow

### 1. Capture Or Import A Fixture

Start from a Support Archive whenever possible. It already contains raw capture
evidence and replay-compatible fixture data from the Home Assistant UI.

If a maintainer needs a custom live capture, keep that workflow local-only and
store the resulting fixture outside git.

If the fixture will be shared, anonymize it:

```bash
python3 tools/anonymize_fixture.py \
  --input /tmp/new_device_fixture.json \
  --output /tmp/new_device_fixture_anon.json
```

Then import it into the local catalog:

```bash
python3 tools/import_fixture.py \
  --input /tmp/new_device_fixture_anon.json \
  --slug new-device-capture \
  --title "New Device Capture"
```

### 2. Build The Detection Path

If the protocol family is new:

- add a new driver under `custom_components/eybond_local/drivers/`
- implement probe, read, and write orchestration
- register it in `custom_components/eybond_local/drivers/registry.py`

Probe logic should:

- verify only the route and address needed for the current family
- read only enough data to establish family and model confidence
- avoid writes during detection
- treat missing optional registers as optional when safe

### 3. Add Register Decoding

For a Modbus-like family:

- add or extend declarative schema JSON under `custom_components/eybond_local/protocol_catalogs/register_schemas/`
- keep family-wide defaults in `base.json`
- use `models/` overlays when differences are model-specific and data-only
- keep block reads contiguous when the protocol requires full-block reads

Keep procedural derived runtime logic in the driver, not in HA entities or schema JSON.

### 4. Add Declarative Capability Metadata

Create or extend a profile JSON under `custom_components/eybond_local/protocol_catalogs/profiles/`.

Prefer shared family-level metadata plus model overlays over copy-pasting full profiles. If multiple variants reuse the same logical controls with different register locations, put the common capability shape into `capability_templates` in the family base and materialize the variant-specific entries from the overlay. If a device is clearly in the same protocol family but still lacks verified write semantics, add a separate read-only fallback profile instead of inheriting a writable default surface prematurely.

The profile should carry:

- groups
- writable capability metadata
- visibility and editability conditions
- presets and recommendations
- support annotations such as `validation_state`, `support_tier`, and `support_notes`

The register schema should carry:

- read-side field layouts
- enum tables
- bit labels
- measurement metadata
- binary-sensor metadata

The Python driver should remain the place for:

- raw transport and protocol decoding
- derived procedural runtime logic
- actual write-command encoding

### 5. Update The Model Catalog

Runtime detection and commercial model administration are separate.

When the work changes which commercial devices are known or supported:

- add or update a model record under `catalog/inverter_models/models/`
- add a sanitized source record under `catalog/inverter_models/sources/`
- link each model variant to the relevant runtime `device_descriptor_keys`
- keep raw private support archives, serial numbers, collector identifiers, IP addresses, and account details out of git
- use durable opaque references (`project-issue:<id>`, `project-attachment:<token>`, `sha256:<hash>`, `fixture:<name>`, `private:<token>` for private conversations or local archives); raw public or private source URLs stay out of the public model catalog unless a record explicitly intends to publish one

Then validate and refresh the journal:

```bash
python3 tools/model_catalog.py validate
python3 tools/model_catalog.py render --output docs/generated/INVERTER_MODEL_CATALOG.generated.md
```

The generated journal is the public support surface. Do not recreate per-family support matrices when the information belongs in the model catalog or the runtime catalog.

### 6. Preserve Partial Protocol Knowledge

If a user provides a full register map, a partial register list, a third-party project mapping, or protocol documentation, store the maintained conclusion in the smallest durable place:

- runtime-safe facts go into `protocol_catalogs/inverter_catalog.json`, profile JSON, register-schema JSON, or tests
- model/support conclusions go into `catalog/inverter_models/`
- private raw material stays local, with only a sanitized source summary committed
- unresolved research notes should be captured only when they remain actionable and privacy-safe

Current SMG-family open reverse-engineering candidates:

- live block registers: `218`, `221`, `222`, `228`, `230`
- config block registers: `304`, `311`, `312`, `317..319`, `328`, `330`, `339`, `340`

These are not blockers for current Home Assistant functionality, but they remain known candidates for future schema work.

### 7. Validate Offline First

Before touching the Home Assistant UX, validate everything against fixtures:

```bash
python3 tools/validate_profiles.py
python3 tools/replay_fixture.py --fixture /path/to/fixture.json --full-snapshot
python3 -m unittest discover -s tests -v
```

If replay fails, fix the driver or metadata before doing HA-level work.

### 8. Add Tests

Minimum expected coverage for a new family or profile:

- profile loader validation
- fixture replay detection and decode
- runtime schema coverage
- control policy coverage
- support metadata coverage
- model-catalog journal sync when the work changes commercial support records

Use the existing tests in `tests/` as the baseline style.

### 9. Polish The Home Assistant UX Last

After the driver and metadata are stable:

- decide which sensors belong in the primary device view
- move noisy helpers into diagnostics
- promote only high-signal summary states
- keep write controls gated by `tested`, confidence, and runtime conditions

Do not hide protocol uncertainty in the UI.

## Acceptance Checklist

Before considering a new driver usable, aim for:

- live or fixture-based detection works
- repeated runtime reads succeed
- runtime UI schema builds correctly
- read-only behavior is acceptable even if writes are not ready yet
- profile validation passes
- unit tests pass
- the public model-catalog journal refreshes cleanly when catalog records changed
- local debug reports refresh cleanly if you use fixture-derived reports
- support level and known limits are documented

## Project Rules

- Do not add raw arbitrary write endpoints.
- Do not put protocol or register knowledge in Home Assistant entities.
- Prefer local fixture-first changes over ad hoc live debugging.
- Keep new family additions JSON-first where possible.
- Treat local fixtures as private debug artifacts until you intentionally anonymize and share them.
