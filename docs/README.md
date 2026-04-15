# Documentation

A guide to the EyeBond Local docs. Pick the path that matches your role.

## I'm a user

Start with the [main README](../README.md). It covers installation, setup, what the integration exposes, and how to get help.

Ukrainian readers can use the [Ukrainian README](../README.uk.md).

If your collector is remote, behind another router, or needs VPN / NAT callback routing, read the [Remote / NAT Setup Guide](REMOTE_SETUP.md).

For deeper hardware compatibility info:

- [SMG Support Matrix](SMG_SUPPORT_MATRIX.md) — what's covered for the SMG / Modbus inverter family
- [Generated SMG Support Matrix](generated/SMG_SUPPORT_MATRIX.generated.md) — auto-refreshed snapshot

## I'm a developer

Start with the [Contributing guide](../CONTRIBUTING.md) for the project philosophy, layering rules, and PR checklist.

Then dive into one of:

- [Adding A New Driver / Profile](ADDING_DRIVERS.md) — fixture-first workflow for extending hardware support
- [Releasing](RELEASING.md) — changelog-first release flow and GitHub release notes generation
- [Tools And CLI Scripts](../tools/README.md) — probing, fixtures, validation, doc generation

## I'm reviewing project status

Generated reports live under [generated/](generated/) and are refreshed by the quality gate:

- [Support Overview](generated/SUPPORT_OVERVIEW.generated.md) — high-level coverage status
- [Generated SMG Support Matrix](generated/SMG_SUPPORT_MATRIX.generated.md) — current declarative support snapshot

Local fixture-derived debug reports are intentionally kept out of the public repository. If you maintain a local catalog under `.local/fixtures/catalog/`, generate those reports locally when needed.

## Refreshing generated reports

Run from the repository root:

```bash
python3 tools/quality_gate.py --refresh-generated
```

Public generated outputs land under `docs/generated/`. Fixture-derived debug reports are intentionally local-only and are not checked in.
