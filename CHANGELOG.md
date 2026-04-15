# Changelog

All notable changes to this project are documented in this file.

The format is inspired by Keep a Changelog, with one practical rule for this repository:
the GitHub release body should be rendered from the matching version section here.

## [Unreleased]

### Added

- Add new user-visible features here.

### Changed

- Add behavior changes, refactors, or support expansions here.

### Fixed

- Add bug fixes and regressions here.

### Docs

- Add documentation-only changes here.

## [0.1.43] - 2026-04-15

### Added

- First public GitHub release of EyeBond Local.
- Built-in local support for SMG / Modbus and PI30-family collectors, plus experimental PI18 replay coverage.
- Support Archive export workflow for unsupported or partially supported inverters.
- SMG writable and readback coverage for registers `341`, `342`, and `343`.

### Changed

- Config-flow runtime copy now loads from private `flow_translations/` bundles while Home Assistant-validated translation files remain Hassfest-compatible.
- Public release validation now passes HACS Validation, Hassfest, and the repository quality gate in GitHub Actions.

### Fixed

- Publication blockers around config schema exposure, manifest ordering, and unsupported translation key placement.

### Docs

- Public README and release metadata were aligned for the first published release.