# Fixture Catalog

Fixture payloads are local-only and are intentionally not committed. The actual catalog lives under `.local/fixtures/catalog/`.

Each catalog entry lives in its own directory:

- `.local/fixtures/catalog/<slug>/fixture.json`
- `.local/fixtures/catalog/<slug>/meta.json`

Conventions:

- `fixture.json` is replay-compatible and may be anonymized.
- `meta.json` stores detected driver/model metadata and import notes.
- `.local/fixtures/catalog/index.json` is generated from all `meta.json` files.

This checked-in README documents the layout only. Keep raw and anonymized fixture payloads out of git unless you explicitly decide to publish them.

Recommended workflow:

```bash
python3 tools/import_fixture.py \
  --input /tmp/eybond_smg_fixture_anon.json \
  --slug smg-6200-live-capture

python3 tools/validate_fixture_catalog.py
```
