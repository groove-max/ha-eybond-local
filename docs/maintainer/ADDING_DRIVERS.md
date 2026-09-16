# Adding A New Driver / Profile

This project is designed to grow through transport-aware payload drivers plus declarative metadata, not through hardcoded model logic inside Home Assistant entities.

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

Optional F (22-byte fixed text), RH (30 bytes, unsigned **8-bit** body sum) and
RB (40 bytes, unsigned **8-bit** body sum, not Q1's 16-bit checksum) are runtime
reads, not additional detection probes. The documented 25-byte RB field layout
and captured 12 zero padding bytes are required; unknown extensions are
rejected. Vendor 19B4 segments 6/7 and the correlated saved exchanges qualify
the non-current RB fields. Segment 7 documents charge/discharge
``multiply=0.1``; those currents and measured ``battery_power``
(V × (Icharge − Idischarge)) publish only while optional RH reports BMS current
display accuracy ``1`` (with decimals) and F ratings are available for I/P
hard-reject bounds. RH ``0``, unread, expired or failed RH omits the keys —
do not publish ÷10 blindly. RH=1 is the discriminator (no retail-model gate).
That RH=1 path is SmartValue-correlated on one live family member; other
members (e.g. Maxinn) are not separately live-proven and only light up if
RH=1.

`short_ascii_optional` owns per-runtime samples, scoped to the transport and
inverter binding. The hub discards this state on recovery; samples are never
persisted as identity. Each successful Q1 cycle performs at most one optional
request (4-second bound), oldest due group first: RB every 30 seconds with a
60-second TTL, F and RH every 900 seconds with a 900-second TTL. Freshness is
checked after the await. Invalid/timeout replies clear that group immediately,
and FULL-result omission removes it from the hub. A failed or cancelled
mandatory cycle, lost connection, changed binding or clock rollback clears all
samples. Only optional failures alongside a successful Q1 count towards the
shared four-strike command cache; the existing re-check action re-enables
requests.

An RB reply with zero voltage and zero SOC explicitly withdraws **all** BMS
measurements/path flags, including nonzero trailing fields seen in the capture.
Positive voltage with zero SOC remains valid. Data availability is not a
physical connection detector. After envelope parse, ``short_ascii_rb_filter``
hard-rejects impossible V/SoC/I/P (pack window, SoC 0–100, current above
1× ``rated_va / rated_battery_voltage``, power only above 3× ``rated_va``).
Link-loss alone may keep last-good publishable while
``age_from_last_good < 180 s`` without refreshing ``sampled_at``; normal RB TTL
stays 60 s. Counters ``bms_link_loss_count`` / ``rb_hard_reject_count`` are
runtime-scoped; ``reading_hold_pending_count`` stays 0 (physics confirmation-hold
deferred). Reference voltage, BMS voltage and ratings have separate owners;
pack-voltage is never inferred from reference voltage. Measured battery DC
watts use published BMS currents; the labelled AC-load estimate uses
load% × rated VA — never substitute one for the other. Optional entities are
disabled by default. Support evidence capture can read MP/Q1/MD/F/RH/RB without
changing command-support state.

Live PV (AABB) admission and inverter controls remain separate work (T4 still
omitted). Do not report full PR/device support based on these fields or a
saved-wire replay alone. Family identity only — no retail-model catalog claim.

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
