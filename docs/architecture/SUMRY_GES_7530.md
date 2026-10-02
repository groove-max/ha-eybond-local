# Sumry/GES 0x7530 qualification

Status on 2026-10-02: evidence-backed offline subset, **not automatic runtime
support**. The Anenji GES48120M250-500P commercial record remains `research` with
no runtime descriptor. The schema `sumry_ges_7530/base.json` is intentionally not
referenced by a detection action, surface or profile. It adds no device I/O,
controls, selection bypass or change to SMG probing. Collector-menu work is
separate from inverter-map qualification.

## Sources and verification

- [Owner's model report](https://github.com/groove-max/ha-eybond-local/issues/49#issuecomment-5881202245)
  and [raw read-only frames](https://github.com/groove-max/ha-eybond-local/issues/49#issuecomment-5881411950).
  The successful FC03 responses corroborate a different map from SMG. Tests
  retain the public telemetry frames without collector/account identifiers.
- [Protocol PDF at community repository commit 76ef3db](https://github.com/quky/Anenji-12KW-48V-Hybrid-split-phas/blob/76ef3db5a5264bbaaa056db135e9ac9b5ad7fa08/documentation/Inverter%20Modbus%20ProtocolV2.6.3-Standard.pdf),
  *Off/ON-Grid Energy Storage Inverter MODBUS Protocol*, V2.6.3, dated
  2025-08-08, 18 pages. A fresh download matched the September 29 copy:
  SHA256 `db454c4e3483639d5922eacbe06f05105f2d931ccdd8afb843daf2b9d6e1835e`.
  Tables on PDF pages 7 and 14 were also visually checked. This is
  community-hosted evidence, not an authenticated supplier release.
- [Owner's fork report](https://github.com/groove-max/ha-eybond-local/issues/49#issuecomment-5944903597)
  and [pinned fork head a4125af](https://github.com/txau/ha-eybond-local/commit/a4125afde16db21ee252da96d32dc966536e285e).
  Static review only: no fork scripts, imports, tests or deployment executed,
  and no raw cherry-pick. Display agreement is owner-reported, not independently
  verified across firmware variants.

## Retained subset

The following meanings agree between the protocol table (PDF pages 7-8) and the
owner's successful address windows. Each read is FC03, three registers, below
the documented 32-register maximum (PDF page 3). Shorter requests are an offline
read-plan choice, not a claim that these exact lengths were captured live.

| Addresses | Meaning | Decode |
| --- | --- | --- |
| `0x7530` / `0x7531` / `0x7532` | Battery voltage / current / SOC | unsigned 0.1 V / signed 0.1 A / unsigned % |
| `0x7548` / `0x7549` / `0x754A` | Phase-A output voltage / current / output frequency | unsigned 0.1 V / signed 0.1 A / unsigned 0.01 Hz |
| `0x756A` / `0x756B` / `0x756C` | Phase-A mains voltage / current / mains frequency | unsigned 0.1 V / signed 0.1 A / unsigned 0.01 Hz |

Battery current preserves the documented native sign: positive discharge,
negative charge. The capture has zero battery current, so tests of either sign
are synthetic protocol checks, not field validation. AC values keep explicit
Phase A labels; they are not whole-device voltage/current or proof of a phase
count. Mains-current import/export direction is not normalized from the
ambiguous translated remarks. No power, energy, total, availability or status
is derived from this subset. Missing blocks stay absent, not zero-filled.

## Why automatic detection is blocked

PDF page 14 defines read-only `0xC738` as model code and `0xC739` as product
class. It does **not** enumerate model-code values. The class values 0, 10 and
20 have translated labels "Single Camera", "Multi-camera" and "Three-phase
camera"; assigning any one to this split-phase GES would be speculation.
The available issue frames and October 1 support archive contain no successful
model/class read. The community ESPHome configuration does not supply a
GES-specific model/class fingerprint either.

Consequently neither plausible 40-70 V plus SOC 0-100 nor an invented
nonzero-model/class-enum test is accepted as identity. Manufacturer text at
`0xC73A` is writable according to the same PDF and is not an immutable anchor.
No model constants, probe action or runtime surface are wired up.

The concrete next evidence is a successful slave-1 **FC03** read of
`0xC738`, count 2, with the original request and CRC-valid response associated
with the reported model. A separate read of `0xC764`, count 5, would preserve
firmware plus protocol-version context (`0xC768`, PDF page 15). These are
evidence requests, not operations performed by this change. Interpret the
returned values against supplier documentation or independently corroborated
model-bound captures before adding exact anchors; one arbitrary nonzero word
is insufficient. No serial read, password, write or collector reroute is needed
for those identity ranges.

For the owner's existing SmartValue Debug raw-RTU path, these are the exact
slave-1 FC03 frames, including low-byte-first CRC. Addresses are raw zero-based
holding-register addresses, not 4xxxx notation:

| Purpose | Start / count | Complete request hex |
| --- | --- | --- |
| Model code and product class | `0xC738` (51000) / 2 | `01 03 C7 38 00 02 78 B2` |
| Protocol version | `0xC768` (51048) / 1 | `01 03 C7 68 00 01 38 A2` |
| Optional firmware plus protocol, instead of the version-only read | `0xC764` (51044) / 5 | `01 03 C7 64 00 05 F9 62` |

Save the unchanged request/response pairs and displayed model/firmware, with
account and serial details hidden. Normal responses start `01 03 04`,
`01 03 02`, and `01 03 0A`, respectively; retain the entire frame and CRC,
including any exception response. A failed read is a remaining blocker, not a
reason for a larger scan. No repeat generic Support Archive is needed for this
missing evidence. These diagnostic requests have not been run against hardware.

## Fork semantics not adopted

- The reviewed head already uses active-power registers `0x7574/0x7575`.
  Criticism of earlier AC V×I-as-watts code does not describe that head.
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

The next runtime implementation can reuse the generic catalog driver and this
tested subset once identity is qualified. It must remain read-only and add
positive/negative identity replays. Broader telemetry needs separately recorded
semantics and missing-data tests; a working fork is not blanket write authority.
