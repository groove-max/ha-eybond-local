# Sumry/GES 0x7530 qualification

Status on 2026-10-02: **exact-match automatic detection** with a **read-only**
runtime surface for Anenji GES48120M250-500P. The commercial record is
`experimental`. Detection uses three immutable holding-register anchors through
the generic `modbus_catalog` driver and binds
`sumry_ges_7530/base.json` (nine phase-A and battery measurements). No write
profile, SMG probing change or collector-menu work is part of this map.

## Sources and verification

- [Owner's model report](https://github.com/groove-max/ha-eybond-local/issues/49#issuecomment-5881202245)
  and [raw read-only frames](https://github.com/groove-max/ha-eybond-local/issues/49#issuecomment-5881411950).
  The successful FC03 responses corroborate a different map from SMG. Tests
  retain the public telemetry frames without collector/account identifiers.
- [Owner identity capture](https://github.com/groove-max/ha-eybond-local/issues/49#issuecomment-5953299167):
  CRC-valid FC03 replies at `0xC738` (model 45, product class 10) and `0xC768`
  (protocol raw 220). These exact values are the catalog anchors.
- [Protocol PDF at community repository commit 76ef3db](https://github.com/quky/Anenji-12KW-48V-Hybrid-split-phas/blob/76ef3db5a5264bbaaa056db135e9ac9b5ad7fa08/documentation/Inverter%20Modbus%20ProtocolV2.6.3-Standard.pdf),
  *Off/ON-Grid Energy Storage Inverter MODBUS Protocol*, V2.6.3, dated
  2025-08-08, 18 pages. A fresh download matched the September 29 copy:
  SHA256 `db454c4e3483639d5922eacbe06f05105f2d931ccdd8afb843daf2b9d6e1835e`.
  Tables on PDF pages 7 and 14 were also visually checked. This is
  community-hosted evidence, not an authenticated supplier release.
- [Owner's fork report](https://github.com/groove-max/ha-eybond-local/issues/49#issuecomment-5944903597)
  and [pinned fork head a4125af](https://github.com/txau/ha-eybond-local/commit/a4125afde16db21ee252da96d32dc966536e285e).
  Useful corroboration for later telemetry expansion; this PR does not adopt the
  broader fork map.

## Identity anchors

All three must match. Missing, truncated, bad-CRC or different values must not
select this entry. Protocol `220` is kept as the raw word; it does not imply the
whole V2.6.3 document applies.

| Purpose | Start / count | Complete request hex | Expected words |
| --- | --- | --- | --- |
| Model code and product class | `0xC738` (51000) / 2 | `01 03 C7 38 00 02 78 B2` | `45`, `10` |
| Protocol version | `0xC768` (51048) / 1 | `01 03 C7 68 00 01 38 A2` | `220` |

Optional firmware context (`0xC764`, count 5) remains a diagnostic read only; it
is not a detection anchor.

## Retained runtime subset

The bound surface exposes only the meanings that agree between the protocol
table (PDF pages 7-8) and the owner's successful address windows. Each runtime
read is FC03, three registers.

| Addresses | Meaning | Decode |
| --- | --- | --- |
| `0x7530` / `0x7531` / `0x7532` | Battery voltage / current / SOC | unsigned 0.1 V / signed 0.1 A / unsigned % |
| `0x7548` / `0x7549` / `0x754A` | Phase-A output voltage / current / output frequency | unsigned 0.1 V / signed 0.1 A / unsigned 0.01 Hz |
| `0x756A` / `0x756B` / `0x756C` | Phase-A mains voltage / current / mains frequency | unsigned 0.1 V / signed 0.1 A / unsigned 0.01 Hz |

Battery current preserves the documented native sign: positive discharge,
negative charge. AC values keep explicit Phase A labels. No power, energy,
total, PV, second-leg, CT, temperature, availability or status is derived from
this subset. Missing blocks stay absent, not zero-filled. No controls are
exposed.

## Still out of scope

- Split-phase second leg, PV strings, CT-only grid semantics, temperatures and
  active-power registers (`0x7574`/`0x7575`, load totals, etc.).
- Derived mains or PV power totals, including one-leg fallbacks.
- Write profiles or Full Control settings.
- Binding from telemetry plausibility alone (battery voltage / SOC without the
  three identity anchors).

Broader telemetry needs separately recorded semantics and missing-data tests; a
working fork is not blanket write authority.

## Fork semantics not adopted

- The reviewed fork head already uses active-power registers `0x7574/0x7575`.
  Its derived `mains_power_total` still accepts one available leg. A future
  total needs qualified topology and every required leg from the current read;
  missing, invalid or stale operands must not become a total. A direct total
  register is preferable where applicable (the PDF documents output totals at
  `0x755E/0x755F`, not a mains total at these addresses).
- The PDF marks `0x7550` onward and `0x756D` onward as three-phase fields.
  The owner's second-leg readings are useful variant evidence, not grounds to
  relabel this entire generic map as split-phase. Also, nonzero words at
  `0x7536..0x753A` conflict with the PDF's reserved area. They remain unmapped.
- `0x7533` battery charging power is unsigned in the PDF, unlike the fork.
  `0x7579..0x757C` are model-dependent temperature sampling points, not proof
  of PV/inverter/transformer/ambient placement. CT installation and active-power
  direction also need variant-specific evidence. These fields are not retained.
