# Adding A New Driver / Profile

This project is designed to grow through transport-aware payload drivers plus declarative metadata, not through hardcoded model logic inside Home Assistant entities.

### MarsRock grid-tie evidence boundary

`marsrock_grid_tie` is a manually selected read-only map, not a SUN-family
autodetection rule. Its sanitized replay fixture contains three complete RTU
sweeps from issue #43. The private serial was replaced and CRCs recomputed;
there are no customer addresses or collector identifiers in the fixture.

The older EyeBond **Modbus RTU V1.0**, pages 10–13, is available through the
[document mirror](https://www.der-kanal-homepage.de/dokumente-sun-1000-gtil)
([PDF](https://drive.google.com/file/d/1Y-eMtkEnyZEeSydIqO6yG1zwy03szFsp/view)).
Its SHA256 is `6c7631c15e39762b21e93d870a220da10653957ca559adaf7bf1b7bf62bdea8e`.
This is not a firmware-specific certification; use only the corroborated fields:

| Zero-based registers | Interpretation | Evidence boundary |
| --- | --- | --- |
| 70 | Grid voltage /10 V | Three exact cloud-record matches |
| 109 | DC input voltage /10 V | Three exact cloud-record matches |
| 86, 87 | Generation power /10 W | Protocol/cloud value; instantaneous LCD equivalence unproven |
| 63, 64 | Total generated energy /10 kWh | Document and cloud total agree |

Each wire word is big-endian; U32 quantities put the **low word first**.
Do not byte-swap the entire register bank: the observed header `2, 256, 1`
differs from the document, while the ASCII serial and measured fields already
decode correctly. That header plus the full ten-character ASCII serial is an
admission check for this explicitly selected map, not unique retail identity.
Other zero-filled fields may be unsupported, so frequency, temperature and
status are not exposed. The observed setting at register 43 also does not match
the documented boolean encoding: there are no write controls or active learning.

Normal polling reads only FC03/slave 1, registers 59–124; identity probing adds
registers 0–19. The usual EyeBond Modbus tunnel `(1, 255)` is a local transport
candidate, **not a devcode proven by the raw cloud capture**. `ModbusSession`
delegates raw versus framed routing to the link adapter. Decoder replay and
HA lifecycle tests do not replace the owner's local-polling confirmation.

The four qualified fields are a minimum, not a declared firmware limit.
The native-RS232 [GTIL2-SmartSolar reader](https://github.com/anhtuanbkfet/GTIL2-SmartSolar/blob/main/src/SunGTIL-ESP/GfSun2000.cpp)
has optional temperature (94), limiter power (99) and output power (107) fields.
Their encodings conflict with V1.0: 107 is reserved there, 94 has an offset,
and 99 uses a different scale. Its basic-mode temperature fallback is not a
measurement. Registers 60 (daily/resettable energy) and 110 (DC current) are
other candidates; no one of these fields is yet qualified for WAL Ver6.1.

Support capture compares individual FC03 reads of those five words with the
59–124 baseline, then samples history at 151/count10, 271/count7, 305/count2,
331/count2 and 451/count5. The whole fixed plan has a 25-second budget, one-second
spacing and no writes. Only Modbus illegal-address exceptions allow continuing;
other errors stop, retaining successful observations. Cancellation propagates.
`read_attempts` keeps UTC request times and monotonic offsets separately from
the unchanged `captured_ranges`/`range_failures` row format. These times are
not inverter measurement timestamps and sequential differences do not prove a
block-read bug. Zero does not prove unsupported. History samples are raw and do
not imply the whole historical area exists or contains valid dates.

`raw_capture.json` retains all successful ranges, failures and unverified
candidate comparisons. `fixture_ranges` retains only the original identity/live
blocks: later point reads must not silently overwrite a baseline or seed runtime
decoding. The comparison is collected only after a driver is bound; it does not
expand auto-detection or the generic failed-identification scan.
The fixture declares ASCII serial registers 3–7 for pseudonymization; the
anonymized replay preserves the qualified header and all measurement words.

Do not substitute the [ESPHome internal-display UART map](https://esphome.io/components/sun_gtil2/)
or Trucki add-on map for the factory EyeBond serial interface.

### Missing export measurements and retained energy

Missing export power must remain unknown. Unallocated PV is not a meter, and
an export-enable flag is not evidence that current export is zero. Explicit
zero readings and an absent grid remain distinct from a missing measurement.
Other estimated flows may still be available when PV-to-grid is unknown.

Derived-energy entities retain accumulated totals in HA's `ExtraStoredData`,
because HA omits custom state attributes while an entity is unavailable. Old
numeric states remain restorable. Missing/disconnected samples reset the
integration baseline; recovery never interpolates across that gap. A daily
counter still observes its local-calendar rollover. Entity IDs and Recorder
history are not rewritten and previously unmeasured energy is not fabricated.

### Cloud protocol compatibility contract

Declare `cloud_probe_codecs` on the driver for read grammars it actually supports.
The compatibility checker uses the driver registry, not a separate model list.
A new dialect also needs a bounded read recognizer in
`support/shadow_learning/protocol_evidence.py`; a cloud name or API device code
is not wire evidence. Preserve all matching grammars when a command is ambiguous.

`cloud_learning_supported` is separate from read/probe support. Set it only when
a real Shadow Learning adapter exists; this flag does not replace current-session
identity, seed, route, restoration or command-protection checks. Inspection may
start on a framed route even when learning cannot, but test dispatch stays closed.

Compatibility probes run through `ReadOnlyProtocolTransport` on the observed
route. A split-route map may declare exact additional `probe_read_plans`, never
an address sweep or a write. A candidate needs the ordinary driver's verified
profile/schema and fresh comparable readings. User confirmation saves route
intent for ordinary re-identification, not fabricated device identity or a write
grant. Raw commands without a verified tunnel route remain diagnostic only.

Cover the codec's valid/invalid reads, rejected writes, ambiguity, cancellation
and wrong-route cases. Include support diagnostics for no-match/unknown results,
without persisting arbitrary unknown payloads or credential-bearing responses.


### Internal auxiliary-channel foundation

The short-ASCII auxiliary channel is not yet exposed by a driver, catalog entry
or discovery route. Its socket-level implementation is shared by the framed and
AT connections. Do not enable it merely because an incoming packet starts with
`AA BB`, or because a collector name or endpoint looks familiar.

The internal `async_send_auxiliary_read` accepts only the two documented
21-byte read queries (subtypes `0200` and `0202`), without a UART bootstrap or
device-setting write. A socket-scoped owner keeps mixed binary framing enabled
after a caller finishes; it never derives the grammar from a live future.
Only the request present at the start of a frame can receive that frame.
Cancellation or timeout after sending closes that exact socket, since these
replies have no transaction identifier. Reconnection starts a new owner.

Integrity or boundary failures close the session. In particular, a valid
EyeBond frame with transaction ID `0xAABB` can overlap the auxiliary grammar:
neither a valid checksum nor an absent waiter resolves that ambiguity. The
current foundation refuses such a frame; it does **not** guarantee auxiliary
availability for every possible payload. Driver-level admission, truthful
model/field semantics and optional-data expiry remain separate work before
user-facing support. Normal connections keep their existing grammar until an
explicit auxiliary read is requested.

Ordinary framed, AT-management and raw-payload sends also pin their physical
writer and run epoch before waiting for request/write locks. `SocketSendOwner`
checks ownership again after bootstrap/spacing and before publishing replies;
an old command cannot resume on a successor. `collector_session_changed` is an
ownership failure, not a command to reconnect or replay a write. A reply that
completed before that same peer closed remains valid if no successor exists.
Disconnect-failed futures are consumed even if a queued write never reached
its response wait. These rules do not select an auxiliary grammar or enable PV.

The same post-reply owner check applies to auxiliary reads, including failures
after replacement. Receive-side retirement is synchronous: disconnect detaches
the old requests, closes that session's lifetime token and writer, and cancels
its reader before awaiting bounded cleanup. Every parser wait is fenced by the
captured session's `read` guard, including the outer timeout and error result;
cancellation alone is not ownership proof. This prevents late AT, framed or
raw bytes and old EOF/timeout diagnostics from changing successor state. The
lifetime guard is active even when auxiliary parsing is disabled. Keep all new
parser awaits inside that guard; do not wrap only the inner socket read of a
`wait_for`, which can itself race with cancellation.

`payload/short_ascii_mppt.py` now decodes an explicitly framed AABB/0200 sample
into an immutable, unit-labelled value object. It does not choose the grammar,
send requests, supply a register schema or populate live telemetry. Settings
0202 are rejected. MPPT voltage/temperature/DC load current remain distinct
from BMS/reference voltage, main-inverter temperature and AC load power.
Unknown enums remain raw codes; zero/maximum words are decoded wire values,
not a proved availability or sentinel policy. The sample has no timestamp or
freshness claim. See the [offline inspector](../../tools/README.md#inspect-a-short-ascii-mppt-frame-offline)
for capture analysis; enabling live PV still requires the admission contract
and per-session optional-sample expiry/invalidation described above.

### Qualified 09C1 baseline

`eybond_09c1` is a separate FC4-only read-only driver, not a relaxed Short-ASCII
parser. Manufacturer `09C1` / decimal devcode `2497` describes the map;
the captured EyeBond routing uses **devcode 1, collector address 255**. Cloud
protocol identity must not be mistaken for the routing devcode.

Commands are plain `Q1`, `QF`, `PV?`, `F`, `G?` plus CR, without a binary address
or PI30/URTU1920 checksum; runtime/support reads also allow optional `PV1`/`PV2`.
Detection still requires only the original four parsed response shapes
(`Q1` 47, `QF` 6, `PV?` 19, `F` 22 bytes). The schema and family descriptor own
the public surface; no collector PN or rating becomes a retail identity.

Q1 owns input frequency; QF owns output frequency. Runtime returns FULL snapshots:
Q1 failure propagates, while missing QF/PV?/F removes only its measurements.
G? text, the ambiguous PV fault glyph, the no-output status bit and the custom
PV energy encoding remain raw support evidence. Rated power is never live power;
the family has no controls even in Full Control. Synthetic tests cover distinct
frequency ownership, scaling, malformed/foreign replies and the real HA lifecycle.

The [issue #50 follow-up](https://github.com/groove-max/ha-eybond-local/issues/50#issuecomment-5945338871)
provides separate PV1/PV2 command captures and two connected local-runtime
snapshots. They qualify the same strict 19-byte PV shape and tenths scaling,
not a capability flag or a physical-port identity. The snapshots/commands are
non-simultaneous; this unit's physical labels are crossed relative to commands.
Neither the supplied LW/GS V1.01 document nor the existing family identity
distinguishes PV1/PV2 support across all variants. Replies have no channel echo:
even two valid replies cannot prove distinct hardware channels or detect a
firmware alias to PV?. Equal/zero readings are valid, not an unsupported test.

The extension therefore uses per-runtime empirical qualification: both commands
must first have valid unexpired samples before either extension channel is
published. Once qualified, failure/expiry removes only that channel. There is
no PV?-to-channel fallback and no inferred total. `QF`/`PV?`/`F` retain their
every-cycle, no-cache semantics. With runtime state, the extension makes at most
one additional four-second-bounded request per successful Q1 cycle; each channel
has a 30-second minimum retry interval. Four consecutive invalid replies/timeouts
use the existing namespaced unsupported cache (`09c1:PV1`, `09c1:PV2`) and explicit
re-check action. Connection loss does not count as unsupported. Stateless
callers use only the legacy read plan, since they cannot retain the retry budget.

Channel values have a TTL fixed at acquisition, `max(60, 3 * poll_interval)`
seconds (60 without a poll interval). FULL snapshots omit failed/expired samples;
`urtu09c1_pv_status` distinguishes `ok` from `cached`, with per-channel age
diagnostics. Cancellation, mandatory failure, observed link loss, clock rollback,
transport/inverter replacement and runtime-state reset clear samples and pair
qualification. These are runtime-scoped samples, not proof of socket continuity;
the payload transport contract has no session-generation identity. Samples are
never re-dated when reused and samples/qualification are never persisted.
Explicit support capture reads both
new commands even when the runtime negative cache skips them and retains raw
malformed/NAK replies for evidence. PV energy/fault semantics and all writes
remain unqualified.

The public manufacturer's map is available as
[09C1 protocol 2497](https://api.valueclouds.com/ppe/api/auth/web/downloadAgreement?devcode=2497).
The qualified XML SHA256 is
`3097e35a53b4f029549b8925a27d3069470b286bb5725d9f5a046aec702abbcd`.
Customer captures remain private; tests use independent synthetic data.

### Qualified short-ASCII baseline

`eybond_short_ascii` is a separate read-only FC4 payload driver. It does not call
the auxiliary API above. It uses the existing catalog probe DAG and requires
all three replies: MP (38 bytes), Q1 (51 bytes with unsigned additive checksum)
and MD (24 bytes including fixed padding). Every query has a fixed timeout;
there is no UART-mode change or fallback to a raw-serial route.

The field layout follows vendor 19B4 segment 1 and saved exchanges from two
devices. Fixed widths, status bits, checksum and envelope are validated before
publishing a complete Q1 snapshot. A failed read raises rather than returning
an empty success. Only static protocol/firmware facts enter identity details.
The MD firmware text and collector PN are not inverter serials or retail
model identifiers. The schema deliberately separates battery reference voltage,
does not mirror output frequency as grid frequency, and leaves the unresolved
Q1 output word in raw support evidence.

The catalog surface is partial/read-only, with no controls profile. Its
confirmed metadata snapshot may persist only with a current matching catalog,
candidate revisions, resolution and evidence fingerprint. Reload must restore
that schema without borrowing default driver controls. Unqualified schema-only
hints remain invalid.

Optional F (22-byte fixed text) and RB (40 bytes, unsigned **8-bit** body sum,
not Q1's 16-bit checksum) are runtime reads, not additional detection probes.
The documented 25-byte RB field layout and captured 12 zero padding bytes are
required; unknown extensions are rejected. Vendor 19B4 segments 6/7 and the
correlated saved exchanges qualify these fields. Segment 5 also describes a
BMS precision setting, but only one variant was captured: RB currents remain
raw evidence until their scaling is established for both variants.

`short_ascii_optional` owns per-runtime samples, scoped to the transport and
inverter binding. The hub discards this state on recovery; samples are never
persisted as identity. Each successful Q1 cycle performs at most one optional
request (4-second bound), oldest due group first: RB every 30 seconds with a
60-second TTL, F every 900 seconds with a 900-second TTL. Freshness is checked
after the await. Invalid/timeout replies clear that group immediately, and
FULL-result omission removes it from the hub. A failed or cancelled mandatory
cycle, lost connection, changed binding or clock rollback clears all samples.
Only optional failures alongside a successful Q1 count towards the shared
four-strike command cache; the existing re-check action re-enables requests.

An RB reply with zero voltage and zero SOC explicitly withdraws **all** BMS
measurements/path flags, including nonzero trailing fields seen in the capture.
Positive voltage with zero SOC remains valid. Data availability is not a
physical connection detector. Reference voltage, BMS voltage and ratings have
separate owners; no current scaling, VA-to-watts or pack-voltage inference is
allowed. Optional entities are disabled by default. Support evidence capture
can read MP/Q1/MD/F/RB without changing command-support state.

AABB/PV admission and inverter controls remain separate work. Do not report
full PR/device support based on these fields or a saved-wire replay alone.

The preferred workflow is:

1. capture or import a local fixture
2. implement or extend driver detection
3. add read-side register schema
4. add declarative profile metadata
5. update the commercial model catalog when support state changes
6. preserve partial protocol knowledge in a durable, privacy-safe form
7. validate offline first
8. polish the Home Assistant UX only after the protocol path is stable

## Design Rule

Prefer the thinnest Python driver that can possibly work.

When deciding where new logic belongs, use this order:

1. `custom_components/eybond_local/protocol_catalogs/profiles/` for capability groups, writable metadata, conditions, presets, and support annotations
2. `custom_components/eybond_local/protocol_catalogs/register_schemas/` for read-side layouts, fields, enums, bit labels, and model overlays
3. `custom_components/eybond_local/payload/` for family-level framing and parse helpers
4. `custom_components/eybond_local/drivers/` only for probe, read, write, and procedural derived logic

Do not add user-facing metadata to a Python driver if the same information can live in JSON.

## Metadata Ownership Rule

When imported SmartESS assets, runtime compatibility overlays, and dedicated
local SmartESS runtime profiles all exist, keep their ownership explicit.

- Raw imported SmartESS asset truth belongs under `custom_components/eybond_local/protocol_catalogs/profiles/smartess_local/models/` and `custom_components/eybond_local/protocol_catalogs/register_schemas/smartess_local/models/`.
- Effective compatibility overlays stay under the runtime family that consumes
  them, for example `pi30_ascii/models/smartess_0925_compat.json`.
- Dedicated local SmartESS runtime profiles stay under `smartess_local/` and
  should not be treated as commercial model names.
- Do not blur SmartESS asset ids, raw inverter model strings, and commercial
  model names. Store commercial support conclusions in `catalog/inverter_models/`.

## Key Project Paths

Core integration code:

- `custom_components/eybond_local/collector/`
- `custom_components/eybond_local/payload/`
- `custom_components/eybond_local/drivers/`
- `custom_components/eybond_local/protocol_catalogs/inverter_catalog.json`
- `custom_components/eybond_local/protocol_catalogs/profiles/`
- `custom_components/eybond_local/protocol_catalogs/register_schemas/`
- `catalog/inverter_models/`
- `custom_components/eybond_local/runtime/`
- `custom_components/eybond_local/canonical_telemetry.py`
- `custom_components/eybond_local/config_flow.py`
- `custom_components/eybond_local/schema.py`

Offline and maintenance tooling:

- `tools/replay_fixture.py`
- `tools/import_fixture.py`
- `tools/validate_fixture_catalog.py`
- `tools/validate_profiles.py`
- `tools/quality_gate.py`

Fixtures and tests:

- `.local/fixtures/catalog/`
- `tests/`

## Recommended Workflow

### 1. Capture Or Import A Fixture

Start from a Support Archive whenever possible. It already contains raw capture
evidence and replay-compatible fixture data from the Home Assistant UI.

If a maintainer needs a custom live capture, keep that workflow local-only and
store the resulting fixture outside git.

If the fixture will be shared, anonymize it:

```bash
python3 tools/anonymize_fixture.py \
  --input /tmp/new_device_fixture.json \
  --output /tmp/new_device_fixture_anon.json
```

Then import it into the local catalog:

```bash
python3 tools/import_fixture.py \
  --input /tmp/new_device_fixture_anon.json \
  --slug new-device-capture \
  --title "New Device Capture"
```

### 2. Build The Detection Path

If the protocol family is new:

- add a new driver under `custom_components/eybond_local/drivers/`
- implement probe, read, and write orchestration
- register it in `custom_components/eybond_local/drivers/registry.py`

Probe logic should:

- verify only the route and address needed for the current family
- read only enough data to establish family and model confidence
- avoid writes during detection
- treat missing optional registers as optional when safe

### 3. Add Register Decoding

For a Modbus-like family:

- add or extend declarative schema JSON under `custom_components/eybond_local/protocol_catalogs/register_schemas/`
- keep family-wide defaults in `base.json`
- use `models/` overlays when differences are model-specific and data-only
- keep block reads contiguous when the protocol requires full-block reads

Keep procedural derived runtime logic in the driver, not in HA entities or schema JSON.

### 4. Add Declarative Capability Metadata

Create or extend a profile JSON under `custom_components/eybond_local/protocol_catalogs/profiles/`.

Prefer shared family-level metadata plus model overlays over copy-pasting full profiles. If multiple variants reuse the same logical controls with different register locations, put the common capability shape into `capability_templates` in the family base and materialize the variant-specific entries from the overlay. If a device is clearly in the same protocol family but still lacks verified write semantics, add a separate read-only fallback profile instead of inheriting a writable default surface prematurely.

When a device reports a documented protocol/layout number, prefer one
protocol-specific family surface over adding an exact catalog model only to
expose telemetry. Keep separate surfaces wherever protocol numbers move or
remove fields. A family surface may attach the matching document-backed control
profile only when every capability is untested and therefore requires an
explicit Full Control choice. Add an exact model record for commercial naming,
model-specific register differences, or controls with stronger validation.

The profile should carry:

- groups
- writable capability metadata
- visibility and editability conditions
- presets and recommendations
- support annotations such as `validation_state`, `support_tier`, and `support_notes`

The register schema should carry:

- read-side field layouts
- enum tables
- bit labels
- measurement metadata
- binary-sensor metadata

The Python driver should remain the place for:

- raw transport and protocol decoding
- derived procedural runtime logic
- actual write-command encoding

### Model-specific write rejection advice

A capability may declare `write_rejection_hint` (plain documentation text) and
`write_rejection_context` (related capability keys in the same profile). Keep
model/manual-specific rules in the exact model overlay, with their operating
conditions and document reference. These are advice, not additional validation,
dynamic UI bounds, proof of write support or permission to adjust other settings.

The protocol driver opts into this advice through `include_profile_hint` on its
error verdict; currently Modbus exception 03 does so. The hub renders it only
after rejection, retaining the original error and cause. Related numeric values
come only from fresh driver telemetry in the pre-write refresh, within a 30-second
refresh-plus-write window. Offline/error snapshots, carried values and reconnects
cannot supply current numbers. No extra reads, writes or rejection retries are
introduced. The original protocol error remains authoritative about what is known.

### Bounded support-only register checks

When a document gives setting addresses but their readback behavior is not yet
confirmed, do not turn them into controls or poll them continuously. A register
schema can instead declare an optional `support_read_plan` with `source`,
`purpose`, `timeout_seconds`, and `blocks` (each with `key`, `start`, `count`).
The generic Modbus catalog driver executes it only while creating a Support
Archive. See `hopewind_0237/base.json` for a concrete example.

This plan supports FC03 only, at most eight blocks, sixteen words per block,
sixty-four words total and fifteen seconds for the whole plan. An explicit
illegal-address reply permits the next declared block; other errors stop the
extra reads, and cancellation propagates. There is no recursive address search.
The result stays in `support_read_diagnostics`, separate from runtime values
and the ordinary replay fixture. An inherited plan can be disabled with `null`.
Successful reads prove readability, not write semantics or tested controls.

### Large register-mapped control surfaces

For a catalog-driven Modbus family with many settings:

- keep each writable capability in the profile and its read-back field in a
  dedicated schema spec set; capability `read_key` values and spec keys must
  match exactly;
- keep on-demand settings ranges in explicitly named blocks (for example
  `control_*`) and never make the normal telemetry reader fetch blocks that do
  not contribute to its requested spec set;
- rotate compact settings blocks through the driver's non-persisted per-session
  runtime state instead of storing a cache in `DetectedInverter.details`;
- update that same runtime cache only after exact wire read-back confirms a
  write, so the coordinator's mandatory refresh sees the confirmed value;
- use the shared capability codec for scaling, packed `HHMM` times, and masked
  register fields. A masked field requires one contiguous 16-bit mask and an
  FC 0x10 read-modify-write; it must never use FC 0x06;
- keep destructive, factory, calibration, address-selection, and ambiguous
  fields out of the profile even when the document marks their register range
  writable.

The profile loader rejects unsupported value kinds, unsafe write functions,
scaled values without exactly one scale, multi-word `time_hhmm`, and masked
FC 0x06 writes. Add loader tests when extending this generic codec contract.

### 5. Update The Model Catalog

Runtime detection and commercial model administration are separate.

When the work changes which commercial devices are known or supported:

- add or update a model record under `catalog/inverter_models/models/`
- add a sanitized source record under `catalog/inverter_models/sources/`
- link each model variant to the relevant runtime `device_descriptor_keys`
- keep raw private support archives, serial numbers, collector identifiers, IP addresses, and account details out of git
- use durable opaque references (`project-issue:<id>`, `project-attachment:<token>`, `sha256:<hash>`, `fixture:<name>`, `private:<token>` for private conversations or local archives); raw public or private source URLs stay out of the public model catalog unless a record explicitly intends to publish one

Then validate and refresh the journal:

```bash
python3 tools/model_catalog.py validate
python3 tools/model_catalog.py render --output docs/generated/INVERTER_MODEL_CATALOG.generated.md
```

The generated journal is the public support surface. Do not recreate per-family support matrices when the information belongs in the model catalog or the runtime catalog.

### 6. Preserve Partial Protocol Knowledge

If a user provides a full register map, a partial register list, a third-party project mapping, or protocol documentation, store the maintained conclusion in the smallest durable place:

- runtime-safe facts go into `protocol_catalogs/inverter_catalog.json`, profile JSON, register-schema JSON, or tests
- model/support conclusions go into `catalog/inverter_models/`
- private raw material stays local, with only a sanitized source summary committed
- unresolved research notes should be captured only when they remain actionable and privacy-safe

See [SMG Protocol Maps](../architecture/SMG_PROTOCOL_MAPS.md) before extending
SMG metadata. Register numbers and protocol numbers must be interpreted within
their specific map, not as a sequence of backward-compatible versions.

Classic SMG-family open reverse-engineering candidates (not a claim that these
addresses are unknown in every protocol):

- live block registers: `218`, `221`, `222`, `228`, `230`
- config block registers: `304`, `311`, `312`, `317..319`, `328`, `330`, `339`, `340`

These are not blockers for current Home Assistant functionality, but they remain known candidates for future schema work.
For example, protocol 2 documents configuration registers 311, 312, 318, 319
and 330; that does not authorize adding their meanings to protocols 1 or 11.

### 7. Validate Offline First

Before touching the Home Assistant UX, validate everything against fixtures:

```bash
python3 tools/validate_profiles.py
python3 tools/replay_fixture.py --fixture /path/to/fixture.json --full-snapshot
python3 -m unittest discover -s tests -v
```

If replay fails, fix the driver or metadata before doing HA-level work.

### 8. Add Tests

Minimum expected coverage for a new family or profile:

- profile loader validation
- fixture replay detection and decode
- runtime schema coverage
- control policy coverage
- support metadata coverage
- model-catalog journal sync when the work changes commercial support records

Use the existing tests in `tests/` as the baseline style.

### 9. Polish The Home Assistant UX Last

After the driver and metadata are stable:

- decide which sensors belong in the primary device view
- move noisy helpers into diagnostics
- promote only high-signal summary states
- keep write controls gated by `tested`, confidence, and runtime conditions

Do not hide protocol uncertainty in the UI.

## Acceptance Checklist

Before considering a new driver usable, aim for:

- live or fixture-based detection works
- repeated runtime reads succeed
- runtime UI schema builds correctly
- read-only behavior is acceptable even if writes are not ready yet
- profile validation passes
- unit tests pass
- the public model-catalog journal refreshes cleanly when catalog records changed
- local debug reports refresh cleanly if you use fixture-derived reports
- support level and known limits are documented
- experimental control surfaces have a user guide that explains opt-in,
  read-back meaning, and deliberate exclusions

## Project Rules

- Do not add raw arbitrary write endpoints.
- Do not put protocol or register knowledge in Home Assistant entities.
- Prefer local fixture-first changes over ad hoc live debugging.
- Keep new family additions JSON-first where possible.
- Treat local fixtures as private debug artifacts until you intentionally anonymize and share them.
