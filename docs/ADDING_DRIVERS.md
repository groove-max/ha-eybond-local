# Adding A New Driver / Profile

This project is designed to grow through transport-aware payload drivers plus declarative metadata, not through hardcoded model logic inside Home Assistant entities.

The preferred workflow is:

1. capture or import a local fixture
2. implement or extend driver detection
3. add read-side register schema
4. add declarative profile metadata
5. validate offline first
6. polish the Home Assistant UX only after the protocol path is stable

## Design Rule

Prefer the thinnest Python driver that can possibly work.

When deciding where new logic belongs, use this order:

1. `custom_components/eybond_local/profiles/` for capability groups, writable metadata, conditions, presets, and support annotations
2. `custom_components/eybond_local/register_schemas/` for read-side layouts, fields, enums, bit labels, and model overlays
3. `custom_components/eybond_local/payload/` for family-level framing and parse helpers
4. `custom_components/eybond_local/drivers/` only for probe, read, write, and procedural derived logic

Do not add user-facing metadata to a Python driver if the same information can live in JSON.

## Key Project Paths

Core integration code:

- `custom_components/eybond_local/collector/`
- `custom_components/eybond_local/payload/`
- `custom_components/eybond_local/drivers/`
- `custom_components/eybond_local/profiles/`
- `custom_components/eybond_local/register_schemas/`
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

If the device can be reached live, capture a stable snapshot first.

Example for Modbus-family devices:

```bash
python3 tools/modbus_dump.py \
  --server-ip <ha_host_ip> \
  --collector-ip <collector_ip> \
  --range <start:count> \
  --fixture-out /tmp/new_device_fixture.json
```

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

- add or extend declarative schema JSON under `custom_components/eybond_local/register_schemas/`
- keep family-wide defaults in `base.json`
- use `models/` overlays when differences are model-specific and data-only
- keep block reads contiguous when the protocol requires full-block reads

Keep procedural derived runtime logic in the driver, not in HA entities or schema JSON.

### 4. Add Declarative Capability Metadata

Create or extend a profile JSON under `custom_components/eybond_local/profiles/`.

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

### 5. Validate Offline First

Before touching the Home Assistant UX, validate everything against fixtures:

```bash
python3 tools/validate_profiles.py
python3 tools/replay_fixture.py --fixture /path/to/fixture.json --full-snapshot
python3 -m unittest discover -s tests -v
```

If replay fails, fix the driver or metadata before doing HA-level work.

### 6. Add Tests

Minimum expected coverage for a new family or profile:

- profile loader validation
- fixture replay detection and decode
- runtime schema coverage
- control policy coverage
- support metadata coverage
- generated-doc sync when the profile affects exported reports

Use the existing tests in `tests/` as the baseline style.

### 7. Polish The Home Assistant UX Last

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
- public generated reports refresh cleanly
- local debug reports refresh cleanly if you use fixture-derived reports
- support level and known limits are documented

## Project Rules

- Do not add raw arbitrary write endpoints.
- Do not put protocol or register knowledge in Home Assistant entities.
- Prefer local fixture-first changes over ad hoc live debugging.
- Keep new family additions JSON-first where possible.
- Treat local fixtures as private debug artifacts until you intentionally anonymize and share them.
