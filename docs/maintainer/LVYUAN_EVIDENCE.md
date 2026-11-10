# LVYUAN TY-SIC-6.5KBE-W1 evidence

What the capture establishes, what it only suggests, and what stays unknown.
Written to keep the pack honest: every claim below is tagged with the evidence
that supports it, so a reviewer can challenge the inference without redoing the
whole analysis.

## Source material

- **Capture**: a passive BLE (btsnoop) trace of the vendor SmartValue app
  talking to the EyeBond dongle on 2026-08-09. 3215 ATT records, 1071 writes and
  1070 notifications, 37 complete poll cycles over 546 s (15.17 s per cycle).
  125 distinct registers. No FC06/FC16 traffic, so nothing here describes a
  write path.
- **Live read**: the same unit over the EyeBond TCP tunnel on 2026-11-10.
- **Cloud snapshot**: the ValueCloud device view for the same hardware.
- All three are sanitized of serial, account, address and collection id before
  they enter the repository. Replay fixtures live in `tests/fixtures/`.

## Confirmed by the capture

| Claim | Evidence |
|---|---|
| Transport is Modbus RTU FC03, slave 1, 9600 8N1 over the transparent tunnel | 1068 CRC-valid request/response pairs, no exceptions |
| The vendor app polls 125 distinct registers every 15.17 s | 37 cycles, stable request set |
| Mains voltage/current/frequency at 80-82, output at 88-93 | decode matches expected magnitudes on every cycle |
| Battery voltage at 128, SoC at 132 | plausible 48 V values across all cycles |
| Register 33290 is an **input-side active power** | mean 1842 W vs 1825 W at output register 91 (99.1 %, i.e. conversion loss); 151 W below mains apparent 1940 VA at PF 0.95; leads output by one cycle; time-aligned correlation 0.58 with mains V x I |
| Counters 448/454/546/552 count in **0.01 kWh** units | 27 counts (0.27 kWh) over 546 s against 0.277 kWh integrated from measured output power, 2.5 % error |
| The counters form the **pairs (448, 454) and (546, 552)** | an increment on one member of a pair coincides with the other member on 94 % of cycles; across pairs agreement drops to 78-89 % |
| Within each pair, the **low member is the daily counter and the high member the lifetime total** | 448/546 read 14.x kWh and fell between August and November; 454/552 read several hundred kWh and only grow |
| The rating block is **not constant** | words 16645 and 16648-16651 are user-settable charge thresholds: 40/480/420/552/544 V in August versus 30/488/480/552/534 V live |
| Word **16646 = 6** is stable across all 37 cycles | single value, every cycle; it is the 6 kW power class |

## Inferred, not proven

These are the claims that must not harden into assertions.

- **Which counter pair is input and which is output.** The 448/546 ratio of
  1.0058 hints that 448 is the input side, but both pairs advance on nearly the
  same cycles, so a passive capture cannot separate direction. The entities are
  therefore named `Daily/Total Energy Counter A` and `B`.
- **Counter word width.** Each counter was read as a single 16-bit word and never
  wrapped, but the capture is too short to prove it is not the low half of a
  32-bit pair. No `combine` is declared, so a wrap would be visible as a drop.
- **Daily reset time.** Values are consistent with a daily counter, but the
  capture does not span midnight, so the reset boundary is unverified.
- **Sign convention of 33290.** It is an input-side power, but whether it is
  grid import, grid export or a bus figure is not established. It is exposed as
  a plain measurement and deliberately **not** published as canonical grid
  power.
- **Words 130/131, 133-135, 143, 147-149, 441-444, 449-452, 547-550, 631-634,
  870/871** are not polled by the vendor app. They are reachable only through
  the support read plan and remain unidentified.

## Unknown

- Serial block 0-8 is ASCII and is not decoded into the model.
- Registers 136-142 always read 32767 and are treated as the unavailable
  sentinel, not as data.
- The register map is **partial**: it is what the vendor app polls, not a
  manufacturer document.

## Consequences in the pack

- The four counters declare **no `state_class`** and are marked diagnostic, so
  Home Assistant stores raw values and derives no energy statistics from an
  unproven series. `total` would still create statistics; only omitting
  `state_class` avoids them.
- 33290 has no canonical mapping for the same reason.
- Detection anchors on word 16646 (exact) and word 16650 as a 500-600 range, so
  a user editing a charge threshold cannot break detection. A 96 V negative
  fixture proves the matcher refuses a same-power-class unit.
- Adding the pack creates Home Assistant poll cycles on top of the vendor app's
  own traffic. The registers are already polled, so no new dongle request shape
  is introduced, but HA does read them independently.
