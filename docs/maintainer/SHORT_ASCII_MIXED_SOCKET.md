# Short-ASCII MIXED socket (protocol memorial)

Internal reference for the framed/AT auxiliary binary channel: EyeBond vs
AABB overlap, claim ownership, and reject rules. Field meanings come only from
`payload/short_ascii_mppt.py` and the framing modules cited below. Do not
invent registers or units that those parsers do not decode.

Out of scope here: cloud reverse proxy, Modbus, and BLE.

## EyeBond 8-byte header

Source: `custom_components/eybond_local/collector/protocol.py`,
`collector/transport/binary_framing.py`.

Layout is big-endian `>HHHBB` over the first eight bytes:

| Offset | Width | Field |
|---|---|---|
| 0 | u16 | `tid` |
| 2 | u16 | `devcode` |
| 4 | u16 | `wire_len` |
| 6 | u8 | `devaddr` |
| 7 | u8 | `fcode` |

`HEADER_SIZE = 8`, `WIRE_LEN_OFFSET = 6`. Derived lengths:

- `total_len = wire_len + 6`
- `payload_len = total_len - 8` (= `wire_len - 2`)

Runtime function codes admitted by `RUNTIME_EYBOND_FCODES`:

| Name | Value |
|---|---|
| `FC_HEARTBEAT` | 1 |
| `FC_QUERY_COLLECTOR` | 2 |
| `FC_SET_COLLECTOR` | 3 |
| `FC_FORWARD_TO_DEVICE` | 4 |
| `FC_TRIGGER_QUERY_REAL_TIME` | 17 |
| `FC_SET_DEVICE_REG` | 18 |
| `FC_TRIGGER_QUERY_HISTORY` | 19 |

`runtime_eybond_header_error(header)` rejects (non-empty reason string):

| Condition | Reason |
|---|---|
| `payload_len < 0` | `collector_frame_length_invalid` |
| `payload_len > 4096` (`MAX_EYBOND_PAYLOAD_SIZE`) | `collector_frame_payload_too_large` |
| `fcode` not in the runtime set | `collector_frame_function_invalid` |

Mechanical `decode_header` stays permissive for tooling; runtime readers share
this error contract.

## AABB 21-byte envelope

Source: `collector/transport/binary_framing.py`.

| Constant | Value |
|---|---|
| Magic | `AA BB` (`AABB_MAGIC`) |
| Frame size | 21 (`AABB_FRAME_SIZE`) |
| Subtypes | `0200`, `0202` (`AABB_SUBTYPES`) |

Prefixes used for MIXED selection are magic + subtype
(`AABB_PREFIXES` = `aa bb 02 00` and `aa bb 02 02`).

`validate_aabb_frame(wire)` requires exact length 21, magic, a supported
subtype at bytes `[2:4]`, and checksum:

```text
sum(wire[2:-1]) & 0xFF == wire[-1]
```

Failures raise `BinaryFramingError` with
`aabb_length_invalid` / `aabb_magic_invalid` / `aabb_subtype_unsupported` /
`aabb_checksum_invalid`. Both subtypes share this envelope; field semantics
differ (see below).

## AABB/0200 runtime fields (`parse_mppt_runtime`)

Source: `payload/short_ascii_mppt.py`. Offsets are whole-wire. Settings
`0202` shares the envelope but is **not** live telemetry:
`wire[2:4] != 02 00` raises `mppt_not_runtime`.

| Offset | Decode | Sample attribute |
|---|---|---|
| 4–5 | u16 BE / 10 | `pv_voltage_v` |
| 6–7 | u16 BE × 10 | `pv_power_w` |
| 8–9 | u16 BE / 10 | `mppt_battery_voltage_v` |
| 10–11 | u16 BE / 10 | `mppt_temperature_c` |
| 12–13 | u16 BE / 10 | `dc_load_current_a` |
| 14 | u8 | `work_mode_code` |
| 15–16 | u16 BE / 10 | `daily_energy_kwh` |
| 17–18 | u16 BE / 10 | `total_energy_kwh` |
| 19 | u8 | `fault_code` |
| 20 | checksum | (validated by `validate_aabb_frame`) |

Named enums (`MpptWorkMode`, `MpptFault`) map known codes only; unknown codes
stay raw. Do not invent additional fields from this memorial.

Documented read queries (auxiliary allow-list) are exactly:

- runtime: `5a a5 02 00` + 16 zero bytes + `02`
- settings: `5a a5 02 02` + 16 zero bytes + `04`

Optional live MPPT uses only the runtime query
(`drivers/short_ascii_mppt_optional.py`).

## MIXED admission and claims

Sources: `collector/transport/auxiliary_session.py`,
`collector/transport/connections.py`, `binary_framing.py`.

Normal framed/AT sessions stay EyeBond-only until an explicit auxiliary read.
`AuxiliaryReadSession.send` sets `enabled = True` only after accepting a
documented read query, then installs an `AuxiliaryReadClaim` **before** the
write. Mix framing (`BinaryGrammar.MIXED`) is used on that socket while
`enabled` remains true.

`AuxiliaryReadClaim` is the ownership record: `(subtype, future)`, captured
before assembling the first response byte. Delivery goes only to the claim
present when the frame started (`accept`). A future merely being alive is
**not** a grammar claim: the codec must not treat waiter liveness as proof of
AABB intent.

Cancellation/timeout after a send fences that exact socket; replies have no
transaction id. Integrity or boundary failures close the session.

## Illegal header resync

Source: `BinaryFrameDecoder` and the non-aux EyeBond header check in
`connections.py` `_read_loop`.

`collector_frame_function_invalid` is fatal on both the non-aux `_read_loop`
and the MIXED `BinaryFrameDecoder`. A 7-byte raw RTU prefix plus the next
frame byte is an illegal function; discarding that window would read another
header that is not buffered.

When an 8-byte EyeBond window fails `runtime_eybond_header_error` with
`collector_frame_length_invalid` or `collector_frame_payload_too_large`
(length-invalid example: `000f02ff0000ff04`) **and** the next header is
already buffered, do **not** close on the first failure. Drop the **entire**
illegal 8-byte window (no tail kept), then read exactly one fresh 8-byte
header. At most **one** discard per frame attempt — never a one-byte slide
(a 32-byte slide budget is wrong: three shifts of `000f02ff0000ff04` ahead of
`aabb…` form the false-legal window `ff0000ff04aabb02` with `fc=2` and
`payload_len=1192`). If the next header is not already buffered, close with
the header error. Do not `readexactly` another 8 bytes to find it.

- Discarded bytes are never published as sensor values and never delivered to
  a waiter.
- `payload_too_large` is decided at the header before any multi-kilobyte
  payload read, including after a discard.
- If the fresh header is legal, read only that payload and continue the
  session.
- If the fresh header is also illegal, close with that reason and publish
  nothing. The close log includes `discarded=` (the dropped window) and
  `header=` (the second window).
- A recovered discard logs `Discarded illegal EyeBond header` with
  `discarded=` and `next_tid` / `next_fc` / `next_payload`. Grep that line
  to prove the known junk header was skipped and a real frame followed.
  A silent framed redial logs `Adopting silent framed redial remote=`.

Do **not** discard through: `binary_frame_ambiguous` (AABB/EyeBond overlap),
bad AABB checksum, or unowned AABB. Those stay session-fatal. A claim-matched
checksum-valid `0200` still publishes.

## Maksym rule (rejects stay rejects)

After a grammar is chosen, checksum, length, function-code, and subtype
failures are rejects. They must not become sensor values. Do not invent fields
to “heal” a bad frame. Sample publication is a separate step after a
structurally valid wire decode.

## Current MIXED overlap behavior (landed)

Source: `BinaryFrameDecoder._select_boundary` in
`custom_components/eybond_local/collector/transport/binary_framing.py`.
The claim is the `auxiliary_claim` snapshot already taken in
`connections.py` `_read_loop` and passed into `async_read_binary_frame`.
A future merely being alive is **not** the claim.

When the session grammar is `MIXED` and the first eight bytes are both:

- a legal EyeBond header (`runtime_eybond_header_error` empty), and
- an AABB prefix (`wire[:4] in AABB_PREFIXES`),

the decoder:

1. If an outstanding `AuxiliaryReadClaim` is present **and**
   `claim.subtype == wire[2:4]`, chooses AABB (21 bytes), then
   `validate_aabb_frame`. Checksum (or other envelope) failure is still
   `BinaryFramingError` (session close) and must not publish a frame.
2. If the claim is `None`, or the subtype does not match, fails with
   `binary_frame_ambiguous`. It does **not** read a speculative longer
   EyeBond tail.

A real EyeBond frame whose TID is `0xAABB` during a matching claim still takes
the AABB path; the 21-byte checksum then fails, the session closes, and no
sensor write occurs. Next reconnect is a normal session. Unsolicited `aa bb`
with no claim stays unpublished (`binary_frame_ambiguous` / unowned). A `0202`
claim must not be decoded as `0200` runtime telemetry: subtype match is
required, and `parse_mppt_runtime` still rejects non-`0200`.

On a 0200 reply, those eight bytes map as EyeBond
`tid=0xAABB`, `devcode=0x0200`, `wire_len` = PV voltage raw word, and `fcode` =
low byte of `(pv_power_w / 10)`. The header is legal when
`(PV watts / 10) mod 256` is in `{1, 2, 3, 4, 17, 18, 19}` (and length stays in
range)—the known collision set.

## Session-fatal vs sample-reject

| Class | Effect |
|---|---|
| Framing / boundary / ambiguous / unowned AABB | `BinaryFramingError` (or related close path): session-fatal; socket recovers on reconnect. |
| RB hard-reject after a good envelope | Omit live BMS keys (`bms_total_voltage`, `battery_soc`, `battery_power`) and set `short_ascii_bms_data_available` false, the same as link-loss. Do not republish the previous live sample inside FULL. |
| MPPT soft fail | Live `pv_power` / `pv_voltage` stay omitted. Labelled `*_held_estimate` mirrors (plus `mppt_held_estimate_age_seconds` / `mppt_held_estimate_mode`) keep the last good sample for the 60 s MPPT TTL. A cached TTL replay projects mode `held` and age since the real sample; only a newly read sample is observed as `live`. |

Checksum/length/subtype rejects never assign sensor values from the failed
contract (Maksym rule). Soft MPPT failure must not be treated as a successful
decode.

## Optional MPPT poller health

Sources: `drivers/short_ascii_mppt_optional.py` (`MpptHealth`, `HEALTH_KEY`),
`drivers/short_ascii_optional.py` (`refresh_one`), framing latch on
`CollectorInfo.mppt_framing_failure_latch` (set in
`collector/transport/connections.py` read loops).

Channel health lives on `runtime_state[HEALTH_KEY]`, **not** inside
`OptionalReads`. `optional_reads_for` rebuilds `OptionalReads` when the
transport object changes or the wall clock goes backward; that rebuild must
not drop health.

States:

| State | Meaning |
|---|---|
| `healthy` | Normal: flaky timeout may take one in-cycle retry; anti-starve may force when stale. |
| `retry_once` | In-cycle only after a flaky first failure; second flaky fail returns to `healthy` and may be due again next Q1 cycle. Must not tight-loop via `next_due=now` plus an immediate second force inside the same refresh. |
| `poisoned` | `poisoned_until` is a `time.monotonic()` deadline (`STRUCTURAL_BACKOFF` = 120 s). Wall-clock steps must not expire or extend poison. |

Transitions:

- Flaky timeout with an empty framing latch → `retry_once` → one more 0200 in the same refresh; second flaky failure → `healthy`, `next_due=now` for the **next** Q1 cycle only.
- Structural `TypeError` (`unsupported_auxiliary_transport`) or MIXED framing kills (`binary_frame_ambiguous`, `aabb_checksum_invalid`) → set `poisoned_until = monotonic()+120` **before** any further await; do not send another 0200 in this cycle; `next_due` uses the long backoff (never `now`). A framing latch is poison even when the soft exception is `TimeoutError` — classify before retrying; never take-clear the latch and then treat it as flaky. A second in-cycle attempt that surfaces a framing latch / structural `TypeError` must poison (not `mark_healthy()`).
- Framing kills latch on the live collector (`mppt_framing_failure_latch`) when `BinaryFramingError` is raised. `CollectorConnection.run()` clears `last_disconnect_reason` on every new connection — the latch is **not** stored there. The poller take-clears the latch synchronously in the exception handler (via `take_mppt_framing_failure_latch` on the facade / mutable test double). On the AT facade, take the framed latch even when `connected` is already false after the kill.
- While `monotonic() < poisoned_until`, anti-starve must not force MPPT even if `last_success_at` is `None` after an `OptionalReads` rebuild.
- When the deadline passes, one attempt is allowed. Another structural failure refreshes the deadline (still not `next_due=now`).
- Checksum-valid successful 0200 → publish the sample. If the framing latch is
  **unchanged** from the pre-solicit snapshot, take-discard it and mark
  `healthy` (stale leftover from a prior kill; a later flaky timeout must not
  false-poison). If the latch is a **new** non-empty value that appeared during
  the successful read (follow-on MIXED kill after `accept`), leave the latch,
  mark `poisoned` (`monotonic()+120`), and do not immediate-retry — the session
  already died after the good frame. Soft MPPT failure must not call
  `record_command_failure`. Aux fence must not wipe Q1.
- Fresh MPPT (success inside TTL) still prefers FC4. Force only when stale **and** healthy **and** RB is also due.

## Pointers

| Concern | Module |
|---|---|
| Header + runtime FCs | `collector/protocol.py` |
| MIXED / AABB / `_select_boundary` | `collector/transport/binary_framing.py` |
| Claim, enable, allow-listed writes | `collector/transport/auxiliary_session.py` |
| 0200 field scales | `payload/short_ascii_mppt.py` |
| Optional 0200 solicit + health | `drivers/short_ascii_mppt_optional.py` |
| Publish / clear / force / poison gate | `drivers/short_ascii_optional.py` (publish, clear, force, and poison). Labelled `*_held_estimate` mirrors on a soft miss are in the session-fatal table above and `drivers/short_ascii_mppt_held.py`. |
| Framing-kill latch | `CollectorInfo.mppt_framing_failure_latch` + connection `take_mppt_framing_failure_latch` |
