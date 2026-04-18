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

## [0.1.50] - 2026-04-18

### Added

- Added a separate deep-scan onboarding path that can probe the full selected IPv4 network from both the first setup step and the scan-results screen.
- Added BusyBox-compatible IPv4 interface parsing for Home Assistant OS, so deep-scan network size and broadcast metadata still resolve correctly when `ip -j` is unavailable.
- Added runtime-schema-aware entity selection for model-specific SMG variants, which restores Anenji PV1/PV2 and other variant-only entities when the detected runtime metadata differs from the generic driver defaults.

### Changed

- The Anenji ANJ-11KW-48V-WIFI-P model-specific write surface is now marked as tested on real hardware, so its validated controls can participate in normal high-confidence `auto` exposure.
- The setup wizard now distinguishes quick scan from deep scan explicitly, with scan-mode-aware hints, timing estimates, and follow-up actions.

### Fixed

- Quick scan now stays effectively broadcast-first by removing duplicate broadcast targets and shortening reverse-connection waits when no UDP reply was received.
- Deep scan no longer reports zero-address networks on BusyBox-based Home Assistant OS hosts and remains available from the results screen even when candidates were already found.
- The scan progress bar now publishes its first determinate update immediately instead of briefly jumping from an indeterminate-looking state.

### Docs

- Updated the English and Ukrainian READMEs, SMG support docs, and generated support overview to describe deep scan, the validated Anenji control surface, and the current onboarding fallback flow more accurately.

## [0.1.49] - 2026-04-17

### Added

- Added a dedicated SMG `family_fallback` runtime path with explicit read-only/unverified markers in the support workflow, support bundle, and exported support archive.
- Added broader built-in Anenji ANJ-11KW-48V-WIFI-P monitoring, including PV1/PV2 telemetry, inverter date/time readback, native PV day/total counters, and a `Sync Inverter Clock` tooling button.
- Added broader SMG read-side diagnostics for the verified default path, including `program_version`, `protocol_number`, `device_type`, `battery_type`, `warning_mask_i`, `dry_contact_mode`, and `automatic_mains_output_enabled`, with cautious hidden-by-default exposure for lower-value raw fields.

### Changed

- SMG writable metadata is now layered through shared family/base/default/model profiles with capability templates instead of one duplicated monolithic profile file.
- The Anenji profile is now shipped as a real 47-capability model-specific control surface, but those writes remain intentionally untested and stay out of normal `auto` exposure.
- Runtime metadata and support reporting now label internal runtime paths separately from commercial hardware names, so docs and exported reports are less misleading.
- The daily grid-export helper now stays available when signed or direct export power keys are present, even if `solar_feed_to_grid_enabled` is missing.

### Fixed

- The default SMG binding now stays limited to verified 6200-class hardware; other SMG-like power classes fall back to the read-only family path instead of inheriting the default write surface.
- Optional SMG probe diagnostics now backfill missing details during normal refresh, so the surviving probe-only sensors remain available after Home Assistant restarts.
- Placeholder all-zero SMG `device_name` values are now suppressed instead of surfacing as misleading identifiers on the verified SMG 6200 path.
- Local draft and SmartESS bridge generation now copy fully resolved profile JSON, so profile shims and layered metadata do not leak into generated local files.

### Docs

- Updated the README, Ukrainian README, SMG support docs, and generated runtime-profile reports to describe the verified default SMG path, the Anenji-specific path, and the read-only SMG family fallback more explicitly.
- Release docs and CLI examples now use the changelog-first flow with version placeholders instead of stale hard-coded historical tags.
- Removed the extra README card badge while keeping the companion card link in place.

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