# Changelog

All notable changes to this project are documented in this file.

The format is inspired by Keep a Changelog, with one practical rule for this repository:
the GitHub release body should be rendered from the matching version section here.

## [Unreleased]

### Added

- Nothing yet.

### Changed

- Nothing yet.

### Fixed

- Nothing yet.

### Docs

- Nothing yet.

## [0.1.48] - 2026-04-17

### Added

- Added optional SmartESS cloud assist for onboarding and diagnostics, including reusable cloud-evidence export for one collector identity.
- Added JSON-first SmartESS protocol and model-binding catalogs plus imported SmartESS assets `0912`, `0921`, and `0925` for metadata ownership, diagnostics, and local draft tooling.
- Added SmartESS local collector helpers for collector query/set commands, protocol-id parsing from query `14`, and known-family metadata planning.
- Added read-only SMG model coverage and wider support-archive capture windows for Anenji ANJ-11KW-48V-WIFI-P / Protocol 3-10 devices.

### Changed

- `Create support archive` is now the main diagnostics flow and can include saved SmartESS cloud evidence automatically or refresh it inline before the ZIP is built.
- Runtime diagnostics, support export, and local draft tooling now resolve effective profile/register-schema ownership from saved or live SmartESS metadata hints, so imported SmartESS assets can be used before a native SmartESS runtime driver exists.
- PI30 default metadata now uses the canonical SmartESS `0925` compatibility paths, while user-facing naming presents raw `VMII-NXPW5KW` devices as PowMr 4.2kW.
- Advanced metadata tools now focus on raw JSON export plus SmartESS draft and bridge generation instead of duplicating routine archive and reload actions.

### Fixed

- Metadata cache priming now also warms catalog-driven metadata, avoiding blocking file reads when Home Assistant starts or reloads local overrides.
- Support archives now store matching SmartESS cloud evidence only once inside the ZIP under `evidence/cloud_evidence.json`.
- Support-archive raw register capture now follows the effective schema name, so model-specific SMG evidence windows are not dropped for variant overlays.
- External relative metadata overrides can now fall back to built-in parent profile and schema files when the local parent file is missing.

### Docs

- Public docs now explain SmartESS cloud evidence, inline archive refresh, and the retention behavior of saved cloud-evidence files.
- Public docs now call out PowMr 4.2kW and Sandisolar SD-HYM-4862HWP as the currently verified commercial examples for the PI30 and SMG families.

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