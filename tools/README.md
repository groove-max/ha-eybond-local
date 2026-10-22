# Tools

Supported repository tools for maintainers and contributors.

Normal users do not need to run these scripts. For hardware problems or
unsupported devices, use the Home Assistant UI to create a Support Archive and
attach it to a GitHub issue.

All commands assume the repository root as the current working directory.

## Build the user documentation site

The website uses a pinned Zensical build, with `website/pages.json` as the
allowlist of public Markdown, images, routes and real translations. Model pages
are views of `catalog/inverter_models/`; do not maintain a second model list.
Only `build/docs/` is a publication artifact. Owner/account setup and private
support materials are not website sources.

```bash
python3 -m pip install -r website/requirements.txt
python3 -m unittest discover -s website -p 'test_*.py'
python3 tools/build_docs.py
python3 tools/check_docs.py
```

For browser verification, install `website/requirements-browser.txt`, run
`python3 -m playwright install chromium`, then `python3 tools/check_docs_browser.py`.
`--live` checks the deployed site; `--render-social` regenerates the social image
from the tracked HTML/SVG source. Browser images stay in `build/docs-browser/`.
The documentation workflow tests pull requests but deploys only from main.

## Validate the project

Use the validation depth appropriate for the current change:

```bash
python3 tools/validate.py plan
python3 tools/validate.py fast
python3 tools/validate.py affected
python3 tools/validate.py unit
python3 tools/validate.py ha --ha-python /path/to/ha-current/bin/python
```

See [the validation workflow](../docs/maintainer/VALIDATION.md) for the release command,
the two HA lanes, and the intended local/CI cadence.

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

The full release flow is documented in [Releasing](../docs/maintainer/RELEASING.md).

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

## Inspect a short-ASCII MPPT frame offline

For maintainers investigating a capture, this command decodes one complete
21-byte AABB/0200 runtime frame under an **explicit format assumption**:

```bash
python3 tools/decode_short_ascii_mppt.py --wire-format aabb-runtime \
  --frame-hex aabb020004b0002502000116001f01001701a400d0
```

The synthetic example represents 120 V PV input and 370 W PV power. The report
separates MPPT battery voltage, MPPT temperature and DC load current from BMS
or inverter measurements. Unknown state/fault codes remain numeric.

This is not a stream scanner: a TCP chunk is not necessarily one frame.
`eybond_header_overlap` warns when the same bytes also resemble an EyeBond
header. Even when it is false, the report does not prove a live session's
protocol or enable polling. Settings replies (`0202`), trailing bytes and
invalid checksums are rejected. Nothing is sent to a device or saved to HA;
keep real captures and reports in ignored `.local/` storage.

## What stays local

Low-level hardware probing, local fixture replay, raw cloud probing, and
case-specific investigation notes are maintainer-only workflows. Keep their
notes and outputs under `.local/`, and do not treat them as user-facing support
steps.
