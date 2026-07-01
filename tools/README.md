# Tools

Supported repository tools for maintainers and contributors.

Normal users do not need to run these scripts. For hardware problems or
unsupported devices, use the Home Assistant UI to create a Support Archive and
attach it to a GitHub issue.

All commands assume the repository root as the current working directory.

## Validate the project

Run the public quality gate used by CI:

```bash
python3 tools/quality_gate.py
```

Refresh generated public docs first, then run the same checks:

```bash
python3 tools/quality_gate.py --refresh-generated
```

Validate declarative runtime profiles directly:

```bash
python3 tools/validate_profiles.py
```

## Maintain the inverter model catalog

Validate catalog records and source references:

```bash
python3 tools/model_catalog.py validate
```

Regenerate the checked-in support journal:

```bash
python3 tools/model_catalog.py render \
  --output docs/generated/INVERTER_MODEL_CATALOG.generated.md
```

Check that the generated journal is current:

```bash
python3 tools/model_catalog.py render --check \
  --output docs/generated/INVERTER_MODEL_CATALOG.generated.md
```

## Cut a release

Render GitHub release notes from `CHANGELOG.md`:

```bash
python3 tools/render_release_notes.py vX.Y.Z \
  --output .local/release-notes/vX.Y.Z.md
```

The full release flow is documented in [docs/RELEASING.md](../docs/RELEASING.md).

## Vet a device contribution

Contribution records are smaller than full support archives and are meant for
learning-derived register maps.

```bash
python3 tools/vet_contribution.py record.json
```

Or build and vet a record from a support archive:

```bash
python3 tools/vet_contribution.py --from-archive support_archive.zip
```

## What stays local

Low-level hardware probing, local fixture replay, raw cloud probing, and
case-specific investigation notes are maintainer-only workflows. Keep their
notes and outputs under `.local/`, and do not treat them as user-facing support
steps.
