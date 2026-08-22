# Cloud learning architecture

Cloud learning is an optional evidence workflow. It is not a runtime telemetry
transport and it never makes cloud data authoritative over local registers.

Two typed registries deliberately model different questions:

- `CloudEvidenceProvider` identifies the cloud ecosystem that owns existing
  evidence for an entry.
- `CloudLearningEngine` identifies the exact API and algorithm used for one
  transient learning run.

More than one learning engine may be compatible with one evidence provider.
The options flow therefore uses the trusted provider only to list compatible
engines. It never chooses an API from a hostname, collector kind, credentials,
or a cloud-family substring. Exactly one engine may declare itself the default
for a provider; an absent or ambiguous default fails closed.

## Current sources

| Source | Purpose | Collector endpoint | Cloud writes | Result |
| --- | --- | --- | --- | --- |
| SmartESS API | Active local learning | Temporary protected shadow route | Bounded control probes under explicit consent | Proven local read/control candidates |
| ValueCloud API | Active local learning | Temporary protected shadow route | Bounded control probes under explicit consent | Proven local read/control candidates |
| DESSMonitor API | Read-only metadata collection | Never changed | None | Typed semantic hints and redacted support evidence |

DESSMonitor is intentionally not an alternate implementation of active shadow
learning. It can expose a broader field catalog, current values, setting names,
and a digest of the latest raw packet, but those facts do not prove a local
register mapping. Consequently its result cannot create an entity, activate a
control, or write a device-scoped overlay.

## Semantic hints are not register bindings

The DESSMonitor adapter classifies each normalized field through the shared
provider-neutral semantic-title catalog. A typed report records the observed
title/value/unit, its source action, and one closed verdict:

- `recognized` means the title has a known canonical meaning and its unit does
  not conflict with the catalog;
- `unit_conflict` keeps the semantic candidate visible but refuses to present
  it as recognized evidence;
- `unknown` preserves the provider field without inventing a meaning.

Every serialized observation carries `local_mapping=unproven`; the report is
versioned, marked `authority=semantic_hint_only`, and requires
`local_mapping_proven=false` when parsed. It contains no register, driver,
writable-capability, or activation field. The DESSMonitor runner still returns
`read_bindings=None`, so neither the read-learning binder nor overlay generator
can consume a cloud title as if it were local wire evidence.

The review UI groups recognized readings, unit conflicts, read-only cloud
setting descriptions, and other fields. A recognized label is useful for the
next correlation stage, but it does not create an entity today.

## Local semantic coverage

After a DESSMonitor run, the options-flow orchestration may compare the typed
semantic report with the coordinator's exact `TypedTelemetryFrame`. This
produces a separate `CloudLocalCoverageReport` that answers a narrower and
immediately useful question: which recognized meanings are already available
from the currently selected local driver?

The coverage report is marked `authority=runtime_semantic_presence_only` and
`local_mapping_proven=false`. It stores semantic keys, freshness/origin/value
kind, and counts, but stores no telemetry values and no register addresses.
An exact semantic-key hit means that Home Assistant already has an independent
local reading; it does not prove that the cloud field maps to that driver's
register. Unknown typed values are not counted as available. Missing, malformed
or old coverage records fail closed and the review falls back to the original
"local source not checked" presentation.

## Typed local register observations

DESSMonitor declares an optional `local_register_snapshot` capability. Before
the cloud fetch, the runtime may ask the currently selected inverter driver for
one bounded live snapshot. The driver — not the cloud adapter, options flow, or
support-package code — owns the exact Modbus tunnel route, function code, slave
address, and register ranges. The shared executor only performs those reads and
records an aware timestamp for each successful block.

`LocalRegisterSnapshot` is bound to the live collector PN and driver key. Its
record includes exact read provenance and raw 16-bit words, and is explicitly
marked `authority=live_local_wire_observation` and
`cloud_mapping_proven=false`. The options flow accepts it only when its PN is
the same identity as the entry PN under the central reconciliation rule. A
foreign, malformed, unsupported, or unavailable snapshot is omitted; the
read-only DESSMonitor run continues without it. Non-Modbus drivers return no
snapshot.

This producer does not parse the older free-form support-capture dictionaries
and does not read the shadow-learning seed map. It also creates no
`read_bindings`, overlay, entity, or write capability. The snapshot is evidence
for a future correlator, not a correlation result.

`LocalRegisterSnapshotSeries` can collect 3–64
ordered snapshots from the same exact collector PN and driver. The sampling
helper has bounded integer count/interval inputs, propagates cancellation, and
fails immediately if identity or driver ownership changes. Each member remains
the original driver-owned snapshot with aware UTC timestamps; the series only
adds ordering and the marker
`authority=repeated_live_local_wire_observation`, and its record still says
`cloud_mapping_proven=false`.

`LocalRegisterCollectionManager` owns a long observation as one retained
coordinator-lifetime task. The DESSMonitor review may start it only after an
explicit checkbox; the foreground options flow returns immediately and may be
closed. The default `LocalRegisterSeriesPlan` takes five snapshots at the
provider's five-minute chart precision (about twenty minutes total): four are
the review correlator's minimum and the fifth is one spare observation. A
dedicated options step exposes typed progress and lets the user cancel or
restart the task.

Every sample still goes through the coordinator's public
`async_capture_local_register_snapshot()` and shared runtime-operation lock, so
polling/writes cannot interleave with one Modbus evidence block. The lock is not
held during the five-minute sleeps. The retained task is cancelled and awaited
before the runtime transport stops. A partial, failed, cancelled, identity-
changed, or driver-changed run never exposes a `LocalRegisterSnapshotSeries`.
The task is deliberately session-scoped rather than persisted across an entry
reload: a reload changes the live driver/transport authority, so silently
resuming an old schedule would weaken its provenance.

## Bounded history and future correlation

The official DESSMonitor API documents read-only daily series through
[`queryDeviceKeyParameterOneDay`](https://api.dessmonitor.com/en/chapter5/queryDeviceKeyParameterOneDay.html)
and same-day chart data through
[`queryDeviceSoleChartEs`](https://api.dessmonitor.com/en/chapter5/queryDeviceSoleChartEs.html).
The read-only DESSMonitor engine now collects a bounded subset of those series
as supplemental evidence. The local producer supplies one real timestamped
register snapshot, but one point cannot establish causality or distinguish
duplicate/static values. The history responses use the device's configured
timezone but do not repeat its offset in each series.

The official read-only `queryDeviceInfo` action provides that missing offset in
seconds relative to UTC together with PN, SN, devcode, and devaddr. A separate
`DessMonitorDeviceTimeBasis` accepts it only when exactly one response row
matches all four parts of the already-resolved cloud identity (PN reconciliation
uses the central same-identity rule). Only that typed object may convert a naive
device-local timestamp to UTC. It never reads the host timezone or current
clock, and a missing, malformed, foreign, or ambiguous row fails closed.

The provider client now has strict read-only models and parsers for those two
official history actions. Each series is identity-bound, bounded, numerically
validated, strictly ordered, and marked
`time_basis=device_local_timezone_unresolved`. It deliberately preserves the
naive provider timestamp instead of converting it to UTC, because doing so
without the device timezone would manufacture false chronology. Duplicate
timestamps, oversized payloads, forged authority markers, and values outside
the requested device-local date fail closed.

`DessMonitorHistoryCollection` is the production collection boundary. One
login session is reused for metadata, the exact-device timezone query, one sole
chart and at most seven key-parameter series. The request date is derived from
an aware UTC clock through the provider-owned offset. A missing or malformed
time basis skips history but preserves metadata; an individual history failure
produces a typed partial result. The serialized record is explicitly read-only,
bounded to eight series, and marks both `local_mapping_proven=false` and
`activation_allowed=false`. The DESSMonitor engine therefore truthfully
declares `history=True`, while route mutation and controls remain disabled.

`DessMonitorResolvedHistorySeries` is the only composition boundary between an
unresolved series and `DessMonitorDeviceTimeBasis`. It requires exact typed
inputs with the same full cloud identity and preserves both the original
device-local timestamp and its canonical derived UTC timestamp. Its serialized
record embeds both source records, verifies every derived point on direct
construction and reload, and remains marked
`authority=provider_identity_bound_time_resolution` and
`local_mapping_proven=false`. A forged offset, foreign identity, altered UTC
time, or altered value therefore cannot become valid resolved evidence.

The pure `CloudLocalHistoryCorrelationReport` is the first comparison policy,
but remains deliberately disconnected from the learning engine. It accepts
only a recognized read semantic, a resolved cloud series, a local series with
the same collector identity, and an explicit bounded alignment tolerance. It
compares each exact Modbus word location independently using only divisors
1/10/100/1000 and observed signed-16 encoding. A candidate requires at least
four aligned samples and at least three distinct cloud values; static data,
short runs, midpoint timestamp ties, non-exact scaling, foreign PN, unknown
semantics, and duplicate matches all fail closed or remain explicitly
ambiguous.

Even `unique_exact_candidate` is serialized under
`authority=review_candidate_only`, `local_mapping=candidate_not_proven`, and
`local_mapping_proven=false`. The report embeds both typed source series and
recomputes the complete verdict when directly constructed or parsed, so a
forged status/candidate/policy count cannot pass the boundary. It imports no
runtime, flow, overlay, activation, or write surface.

After a user-started local observation completes, a later DESSMonitor run may
compose its newly fetched identity-bound history with that completed series.
The pure `CloudLocalHistoryReview` stores both evidence inputs once and emits
compact, recomputed verdicts. Its alignment tolerance is half of the tighter
observed cadence (150 seconds for the default five-minute collection), so
nearest-point windows do not overlap and exact midpoint ties remain rejected.
The aggregate is marked `authority=review_composition_only`, `read_only=true`,
`local_mapping_proven=false`, and `activation_allowed=false`. It is attached
only to the transient review/support evidence; the DESSMonitor runner,
coordinator, runtime, overlay generator, entities, and write paths do not import
or consume it. Waiting still happens only in the retained coordinator task,
never in the foreground options step. A completed series is intentionally
session-scoped: after reload the user starts a new observation rather than
silently reusing stale local evidence.

The next boundary is a full-route representability review, not an overlay
generator. The coordinator projects an immutable snapshot of the currently
identified inverter's exact `ProbeTarget`, effective driver/schema identity,
already claimed `(function, register)` words, and existing semantic keys. The
pure review compares every correlation verdict against that snapshot. A unique
candidate is compatible only when collector identity, driver, `devcode`,
collector address, and payload device address still match; function code and
register remain explicit. Driver drift, route drift, an existing semantic, a
claimed register, and an ambiguous correlation are separate closed verdicts.
The record is marked `authority=current_context_review_only`,
`draft_generation_allowed=false`, and `activation_allowed=false`. It neither
calls the legacy overlay generator nor writes metadata.

The downstream `CloudLocalReadDraftPlan` is the first boundary allowed to say
that an **inactive** draft can be generated. It consumes only the exact typed
representability review and recomputes every item from the embedded history,
semantic, route, and schema-context evidence. Only a unique representable
candidate may enter the plan. If two cloud series claim the same exact route
and register, one semantic claims multiple locations, or a source key is
duplicated, all colliding items remain review evidence but are excluded from
the plan. Its record is marked
`authority=inactive_review_draft_plan_only`,
`local_mapping_proven=false`, and `activation_allowed=false`.
`draft_generation_allowed=true` means only that a later writer may create a
reviewable inactive artifact; it does not prove the register mapping and does
not permit activation. The plan module imports no runtime, config flow, local
metadata writer, overlay generator, or activation surface. The options-flow
metadata compositor attaches a fresh plan beside the exact review and removes
any stale or forged plan when either prerequisite cannot be reconstructed.

An explicit downstream writer can turn that plan into one deterministic local
register-schema file under `learned/dessmonitor_review/`. Immediately before
writing, it reloads the named source schema and rebuilds the complete overlay
context; direct-constructor forgeries and schema/driver/claimed-register drift
therefore fail before the local metadata directory is created. The file keeps
the exact FC, register, divisor, signedness, semantic metadata, route context,
and evidence digest. Its generated measurement keys use the guarded
`learned_read_` prefix and are disabled by default. The write is an atomic
single-file replace and an exact rerun is idempotent; an existing different
file is never overwritten. The artifact remains marked
`authority=inactive_review_artifact_only`, `local_mapping_proven=false`, and
`activation_allowed=false`. It creates no profile, changes no config entry,
requests no reload, and has no path to runtime activation.

The DESSMonitor result screen exposes this writer as a separate explicit
**Create an inactive local sensor draft** action only when the exact adjacent
plan contains at least one safe item. It is not the active learner's **Apply**
action. The flow compares the current runtime overlay context with the plan
again, runs the synchronous writer in Home Assistant's executor, and reports
that no entities were added. A stale/forged plan or changed context exposes no
writer action and performs no filesystem operation.

## Active shadow-read route authority

The existing SmartESS-compatible active learner has a separate, stricter path
for read sensors. The shadow backend records each sampled word as
`ShadowReadRegisterEvidence`: EyeBond `devcode`, wrapper collector address,
Modbus payload unit, read function (FC3 or FC4), register, and bounded samples.
The historical address-only `read_map.registers` projection remains available
for diagnostics and contribution compatibility, but the active binder never
consumes it.

`ReadBindingCandidate` preserves the same full address. Equal values at the
same register under another function, unit, or EyeBond route are distinct
candidates and therefore ambiguous; they are never merged. The overlay
generator receives a typed `LearnedReadActivationContext` projected from the
currently identified inverter. It emits a read spec only when the candidate's
route equals that current `ProbeTarget`, and writes the exact function into the
schema. A raw passthrough session without an observed EyeBond wrapper cannot
prove the complete route and therefore generates no active learned reads.

Selection is not activation authority by itself. Before persisting an
activation, the coordinator reloads both generated files, requires the profile
and schema to carry the same manifest, and verifies every selected key against
the schema's exact spec-set, FC, register, learned measurement, and route
location. It then compares the persisted read context with the current
collector PN, driver, base schema, and `ProbeTarget`. Runtime metadata
resolution repeats the route check when a concrete inverter target is
available. Legacy activations with no explicit read selection expose zero
learned read sensors. This boundary is read-specific; learned controls retain
their existing independently proven write path.

This does not grant activation authority to DESSMonitor history correlation.
Its representability result and inactive draft plan remain review-only until a
separate evidence boundary proves a mapping and an explicit user action selects
it for activation.

## Trust boundaries

Every engine run is bound to the entry's exact normalized collector PN. The
provider may reconcile a short and full PN only through the central
same-identity rule. A missing, foreign, or ambiguous cloud device fails before
metadata collection.

The source selector is transient flow state. Source choice, username, password,
session token, and signing secret are never written to config-entry data or
options. Normalized evidence contains no credential or signed-session material.
Raw cloud packets are represented only by length and SHA-256 digest.

Route mutation is capability-gated. The flow may start, stop, or fail-safe a
shadow route only when the exact selected engine declares
`requires_shadow_route=True`. A metadata-only engine receives inert route and
learning callbacks; success, failure, and cancellation all perform zero endpoint
operations.

## Persistence and support artifacts

Learning results remain transient until one of two explicit actions:

- active-learning results may be selected and activated as a device-scoped
  local overlay;
- any result may be exported in a Support Archive.

Read-only metadata is shown to the user and included as support evidence. It is
not promoted into built-in metadata or local entities automatically. Catalog
support still requires review and a separately proven local protocol mapping.
When available, the typed local register snapshot is exported in that same
sanitized support evidence; collector identity is masked and the bounded raw
words remain available for offline analysis.

## Load-bearing checks

- `test_cloud_learning_engines.py` checks strict models, exact source resolution,
  compatibility, and one declared default.
- `test_shadow_learning_backend.py`, `test_read_learning_binder.py`, and
  `test_shadow_learning_overlay_generator.py` check full-route read evidence,
  FC3/FC4 separation, route-aware ambiguity, schema function preservation, and
  fail-closed generation/activation inputs.
- `test_device_scoped_overlay_activation.py` and `test_effective_metadata.py`
  check explicit read selection and persisted/current `ProbeTarget` matching.
- `test_dessmonitor_cloud.py` checks signing, exact-PN binding, read-only actions,
  bounded evidence, and credential non-disclosure.
- `test_dessmonitor_history.py` checks the two official history actions, strict
  unresolved-time records, bounded parsing, signed read-only requests, and the
  absence of any local binding/runtime dependency.
- `test_dessmonitor_time_basis.py` checks exact-device timezone ownership,
  strict offsets, deterministic UTC conversion, forged-record rejection, and
  the read-only `queryDeviceInfo` request.
- `test_dessmonitor_history_resolution.py` checks exact-identity composition,
  canonical UTC derivation, byte-stable records, forged-derived-point refusal,
  and the absence of runtime, driver, binding, or activation authority.
- `test_dessmonitor_collection.py` checks single-login metadata/history reuse,
  device-local date resolution, strict series bounds, partial failure handling,
  JSON stability, and the read-only non-activation boundary.
- `test_dessmonitor_learning.py` checks that the read-only runner never opens a
  route or invokes a learning writer, and emits only unproven semantic and
  bounded historical evidence.
- `test_cloud_semantic_evidence.py` and `test_dessmonitor_semantics.py` check the
  strict typed report, catalog classification, JSON roundtrip, and absence of
  local-mapping authority.
- `test_cloud_local_coverage.py` checks exact typed-frame reconciliation,
  freshness, unknown-value handling, JSON roundtrip, value non-disclosure, and
  the presence-only authority marker.
- `test_local_register_evidence.py` checks strict models, aware timestamps,
  exact Modbus route/function execution, cancellation, partial-read handling,
  JSON stability, and the unproven authority marker.
- `test_driver_local_register_evidence.py` checks that each supported Modbus
  driver owns its plan and that catalog function-code provenance is preserved.
- `test_local_register_series.py` checks bounded repeated capture, exact
  identity/driver continuity, ordering, cancellation, JSON stability, and the
  absence of cloud, runtime, binding, or activation authority.
- `test_local_register_collection.py` checks strict plans/status, retained-task
  ownership, busy refusal, cancellation before and during execution, closed
  failure reasons, support projection, and the absence of cloud/correlation or
  activation authority.
- `test_cloud_local_history_correlation.py` checks exact temporal alignment,
  unique/ambiguous/static/insufficient/no-match verdicts, signed and decimal
  scaling, exact identity/semantic gates, compact multi-series review
  composition, derived-record revalidation, and the review-only non-activation
  boundary; it also checks full-route representability, driver/route/schema
  drift and exact context roundtrip. It also checks inactive-plan derivation,
  route/scale preservation, cross-series conflict exclusion, byte-stable
  revalidation, forged-item refusal, pre-write schema-context reconstruction,
  deterministic atomic artifact generation, loader validity, overwrite
  refusal, and the no-activation boundary.
- `test_config_flow.py` checks source UX, consent separation, transient state,
  metadata review, and zero endpoint cleanup on metadata-only failure/cancel.
- `test_cloud_evidence_architecture.py` keeps provider resolution separate from
  learning-engine execution.
