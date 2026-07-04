# Contributing to EyeBond Local

Thanks for your interest in improving EyeBond Local. This document covers the developer workflow, project conventions, and what to know before opening a PR.

If you're a user looking to install or troubleshoot the integration, the [README](README.md) is the right place to start.

---

## Project Philosophy

EyeBond Local is intentionally layered around clear boundaries:

```
transport  →  payload  →  driver  →  profile  →  register schema  →  HA entities
```

Each layer should know as little as possible about the layers above and below it. The most important rule:

> **Prefer the thinnest Python driver that can possibly work.**

Anything that can live in JSON (capabilities, conditions, presets, register layouts, enum tables) **should** live in JSON, not in Python. Hardware metadata is data, not code.

---

## Layer Responsibilities

When deciding where new logic belongs, use this order:

1. **`protocol_catalogs/profiles/`** (JSON) — capability groups, writable metadata, conditions, presets, support annotations.
2. **`protocol_catalogs/register_schemas/`** (JSON) — read-side layouts, fields, enums, bit labels, model overlays.
3. **`payload/`** (Python) — family-level framing and parse helpers.
4. **`drivers/`** (Python) — probe, read, write, and procedural derived logic only.
5. **HA entities** — purely presentational. No protocol or register knowledge.

---

## Development Workflow

### Fixture-first

When live hardware access is limited, use fixtures. The full workflow for adding a new driver or profile is documented in [docs/ADDING_DRIVERS.md](docs/ADDING_DRIVERS.md).

The short version:

1. Capture or import a fixture.
2. Build or extend driver detection.
3. Add register schema.
4. Add capability profile.
5. Validate offline.
6. Polish the Home Assistant UX last.

### Repository layout

```
custom_components/eybond_local/
├── collector/              # UDP discovery + TCP server
├── connection/             # connection types and setup form metadata
├── onboarding/             # autodetection and presentation
├── runtime/                # coordinator, hub, transport orchestration
├── payload/                # protocol framing and parsing
├── drivers/                # probe + read + write orchestration
├── protocol_catalogs/
│   ├── profiles/           # capability metadata (JSON)
│   └── register_schemas/   # register layouts (JSON)
├── fixtures/               # runtime fixture/replay helper package
├── support/                # support exports and evidence indexes
├── metadata/               # profile/schema loaders and local drafts
└── *.py                    # entity platforms, config flow, services

docs/                       # public docs and generated reports
catalog/inverter_models/    # commercial model and sanitized source records
tools/                      # CLI utilities, validation scripts, and release helpers
.local/                     # maintainer-only notes, fixtures, design history, generated reports, and release scratch files (gitignored)
tests/                      # unit and regression tests
```

---

## Validation

Run the full check from the repository root:

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall custom_components/eybond_local
python3 tools/quality_gate.py
```

To refresh generated documentation reports before running the quality gate:

```bash
python3 tools/quality_gate.py --refresh-generated
```

`pytest tests/` works too (`pytest.ini` disables the auto-plugins that
conflict with the suite), and with `pytest-xdist` installed
(`pip install pytest-xdist`) `pytest tests/ -n auto` runs it in parallel —
that plugin is optional and not part of the base environment.

The quality gate checks profile validity, unit tests, and public generated-doc freshness. PRs should leave it green.

Fixture replay and fixture-derived reports are local-only workflows. Keep those artifacts under `.local/` and run them manually when you need debugging evidence.

A short reference for supported validation, catalog, release, and contribution
tools is in [tools/README.md](tools/README.md). Low-level hardware probes and
local fixture workflows are maintainer-only notes under `.local/`.

---

## Project Rules

These are the hard rules — don't cross them without discussion:

- **No raw arbitrary write endpoints.** All writes must go through declared, gated capabilities.
- **No protocol or register knowledge in HA entities.** Entities are presentation only.
- **Fixture-first over live debugging** when feasible — but keep those fixtures local unless you intentionally sanitize them for sharing.
- **JSON-first over Python** for new family additions.
- **Local fixtures and maintainer-only notes belong under `.local/`** and are not part of the public repository.
- **`tools/` is supported project tooling** for CI, release validation, catalog validation, generated-doc checks, and sanitized contribution workflows.
- **`.local/generated/`, `.local/release-notes/`, and raw research dumps** remain local-only artifacts and should stay out of git.

---

## Reporting Issues

If you're a user reporting a problem, see the [Getting Help](README.md#getting-help) section in the README.

If you're a contributor working on a fix, please attach:

- the relevant test fixture (anonymized if it came from real hardware)
- a unit test that reproduces the issue
- a Support Archive if the bug is detection-related

---

## Pull Request Checklist

Before opening a PR:

- [ ] Tests pass (`python3 -m unittest discover -s tests -v`)
- [ ] Quality gate passes (`python3 tools/quality_gate.py`)
- [ ] New driver / profile / schema additions follow the layering rules above
- [ ] Generated docs refreshed if your change affects exported reports
- [ ] No new dependencies in `manifest.json` requirements (the integration is intentionally dependency-free)
- [ ] No private `.local/` artifacts committed by accident

---

## License

By contributing, you agree that your contributions will be licensed under [MPL-2.0](LICENSE), the same license as the rest of the project.
