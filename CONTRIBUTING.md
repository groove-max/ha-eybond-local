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

1. **`profiles/`** (JSON) — capability groups, writable metadata, conditions, presets, support annotations.
2. **`register_schemas/`** (JSON) — read-side layouts, fields, enums, bit labels, model overlays.
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
├── profiles/               # capability metadata (JSON)
├── register_schemas/       # register layouts (JSON)
├── fixtures/               # offline replay helpers
├── support/                # support exports and evidence indexes
├── metadata/               # profile/schema loaders and local drafts
└── *.py                    # entity platforms, config flow, services

docs/                       # public docs and generated reports
tools/                      # CLI utilities and maintenance scripts
.local/fixtures/catalog/    # local replay fixtures (gitignored)
tests/                      # unit and regression tests
.local/                     # ignored local research artifacts
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

The quality gate checks profile validity, unit tests, and public generated-doc freshness. PRs should leave it green.

Fixture replay and fixture-derived reports are local-only workflows. Keep those artifacts under `.local/` and run them manually when you need debugging evidence.

A complete reference of CLI tools (probing, local fixtures, validation, doc generation) is in [tools/README.md](tools/README.md).

---

## Project Rules

These are the hard rules — don't cross them without discussion:

- **No raw arbitrary write endpoints.** All writes must go through declared, gated capabilities.
- **No protocol or register knowledge in HA entities.** Entities are presentation only.
- **Fixture-first over live debugging** when feasible — but keep those fixtures local unless you intentionally sanitize them for sharing.
- **JSON-first over Python** for new family additions.
- **Local fixtures belong under `.local/fixtures/catalog/`** and are not part of the public repository.
- **`.local/`** is for private research, session notes, raw field dumps, and local debugging artifacts. It's gitignored on purpose.

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
