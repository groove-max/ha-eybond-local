# Universal Inverter Catalog Refactor

## Goal

Replace protocol-specific identification paths with one compiled, explainable,
read-only detection architecture.

The source catalog remains declarative. Runtime detection uses an in-memory
compiled catalog with indexes and a decision DAG. Device identity and runtime
surface resolution are separate decisions.

## Architectural Model

### Protocol Descriptor

Describes how evidence can be collected for one protocol family:

- transport and route requirements;
- read-only probe actions;
- parser keys;
- evidence fields produced by each action;
- timeout, retry, optionality, and relative cost.

Supported probe action kinds:

- `modbus_read`;
- `ascii_command`;
- `smartess_query`;
- `collector_metadata`.

No detection action may write to the collector or inverter.

### Device Descriptor

Describes one physical model, hardware/firmware revision, compatible model
group, or family fallback:

- stable key and display aliases;
- protocol key;
- required, supporting, and contradicting evidence;
- provenance and confidence;
- runtime validation policy;
- selected runtime surface.

### Surface Descriptor

Describes the runtime behavior that can safely be exposed:

- driver key and variant key;
- profile and register schema references;
- support tier;
- read/write policy;
- optional diagnostics;
- support-capture policy.

Several device descriptors may share one surface descriptor.

### Resolution Result

Detection returns:

- candidate device keys;
- `exact`, `compatible_group`, `family`, or `unresolved` resolution;
- selected surface key when safe;
- confidence independent from resolution level;
- collected, missing, failed, and contradicting evidence;
- decision path and catalog revision.

The first matching descriptor is never selected implicitly.

## Evidence Rules

- Missing evidence is not negative evidence.
- Read errors, unsupported actions, missing values, and observed values are
  distinct states.
- Configurable settings and runtime state cannot be required identity anchors.
- Local immutable evidence outranks local structural evidence.
- SmartESS metadata is optional supporting evidence and cannot override
  contradicting local immutable evidence.
- Multiple unresolved devices may select a common surface only when every
  remaining candidate declares that same compatible surface.

## Runtime Compilation

At integration startup or metadata reload:

1. Load and validate the source catalog.
2. Resolve profile and schema references.
3. Compile protocol actions, evidence definitions, device descriptors, and
   surfaces.
4. Build indexes by protocol, transport, exact evidence, alias, device key,
   surface key, and evidence key.
5. Build a decision DAG for each protocol.
6. Cache the compiled catalog for the process lifetime.

Each detection session separately caches probe responses and failures so the
same physical action is not repeated.

The runtime executor evaluates the decision DAG after every action. It requests
the cheapest action that produces the next required evidence key, records
failed and unsupported evidence separately, and stops model probing when the
remaining candidate set is safe. Mandatory non-identification actions required
to construct the runtime object are then executed from the same session cache.

## Persistence

Persist the last confirmed:

- device candidates and resolution level;
- selected surface key;
- confidence;
- evidence fingerprint;
- source catalog version and descriptor revisions.

Persisted data is a safe startup hint, not a substitute for live confirmation.
Entity reconciliation remains blocked until a valid persisted surface or live
resolution is available.

A catalog-bound persisted snapshot is valid only when:

- its catalog version matches the loaded catalog;
- its surface still exists and resolves to the same driver, variant, profile,
  and register schema;
- all persisted candidate descriptors still exist;
- persisted descriptor revisions match their compiled revisions.

Legacy snapshots without catalog metadata remain readable for one-way
migration, but every new live confirmation writes the complete catalog-bound
snapshot.

## Source Layout

Target logical layout:

```text
protocol_catalogs/
  inverter_catalog.json
profiles/
  <protocol-or-family>/
register_schemas/
  <protocol-or-family>/
```

Profiles and register schemas remain separate payload files because they are
large, reusable, inheritable metadata. A device descriptor owns them through a
surface reference.

## Migration Phases

### Phase 1: Generic Compiled Core

Status: complete.

- Introduce generic probe-action, evidence, surface, device, and resolution
  types.
- Compile the existing SMG catalog into the generic runtime representation.
- Make SMG identity probing consume the compiled protocol plan.
- Preserve current SMG behavior and support-package diagnostics.

### Phase 2: Canonical Source Schema

Status: complete.

- Convert the current SMG source into the final universal catalog schema.
- Move support-capture policy into surface descriptors.
- Remove SMG-specific onboarding and support constants.
- Remove duplicate SMG entries from `model_bindings.json`.

Implemented in `protocol_catalogs/inverter_catalog.json`:

- canonical `protocols[]` with read-only probe actions and protocol-owned layouts;
- reusable `surfaces[]` with stable keys and driver defaults;
- surface-owned support-capture policies;
- device and family descriptors referencing surfaces by key;
- SMG metadata removed from `model_bindings.json`;
- SMG onboarding confidence derived from compiled surface/provenance metadata.

### Phase 3: PI30 Migration

Status: complete.

- Express `QPI`, `QPIRI`, `QPIGS`, `QFLAG`, and `QMOD` as ASCII probe actions.
- Migrate `pi_family.json` predicates into device/surface descriptors.
- Move PI model aliases and fallback behavior into the catalog.
- Delete the standalone PI resolver path.

### Phase 4: PI18 Migration

Status: complete.

- Express PI18 commands as ASCII probe actions.
- Add a PI18 family descriptor and compatible read-only surface.
- Add concrete model descriptors only when evidence exists.
- Remove literal protocol checks from the driver.

### Phase 5: SmartESS Evidence

Status: complete.

- Express SmartESS queries and collector metadata as optional evidence actions.
- Keep SmartESS protocol assets distinct from physical device identity.
- Map assets to candidate devices/surfaces as supporting evidence.
- Remove the standalone effective-metadata ownership path for SmartESS hints.

### Phase 6: Consolidation

Status: complete.

- Remove `model_bindings.json`.
- Remove `pi_family.json`.
- Remove profile compatibility wrappers such as `smg_modbus.json` after
  persisted-name migration.
- Flatten unnecessary `.variant.json` wrappers.
- Remove protocol-specific detection constants and duplicate resolvers.
- Replay the support-package corpus and validate on real devices.

Implemented:

- PI30 and PI18 probes execute compiled `ascii_command` actions;
- SMG, PI30, and PI18 probe routes and timeout budgets come from protocol
  descriptors;
- the decision DAG actively selects probe actions at runtime instead of serving
  only as diagnostics;
- each session caches executed, failed, and unsupported actions/evidence;
- optional firmware, serial, rated-power, SmartESS, and variant actions are read
  only when required by the active candidate branch;
- PI30 OR predicates are separate prioritized descriptors sharing surfaces;
- PI18 is a normal read-only driver with a family surface;
- SmartESS protocol assets are optional supporting evidence and never own runtime metadata;
- local PI evidence outranks SmartESS hints;
- runtime-state values are not required identity anchors;
- ambiguous fingerprints and overlapping family defaults are rejected during
  catalog validation instead of being resolved by source order;
- compiled indexes cover protocol, transport, evidence key, exact evidence,
  alias, device key, and surface key;
- resolution results contain collected, missing, failed, unsupported, and
  contradicting evidence, decision path, catalog version, descriptor revisions,
  and evidence fingerprint;
- persisted effective metadata includes candidates, resolution level, surface,
  evidence fingerprint, catalog version, and descriptor revisions;
- standalone PI and model-binding catalogs and loaders are removed;
- SMG variant profile wrappers are flattened;
- the legacy `smg_modbus.json` persisted name resolves through a loader alias;
- new runtime resolutions and snapshots use canonical
  `modbus_smg/default.json`, while legacy snapshots remain readable;
- SMG runtime selection and family fallback are derived only from compiled
  catalog resolutions; the source loader no longer performs a second match;
- `smartess_query` actions execute through optional injected evidence providers
  and remain unsupported, rather than required, when no provider is available;
- all built-in protocols resolve through `inverter_catalog.json`.

The legacy `descriptor_decision_shadow` support-package member is retained as a
backward-compatible diagnostic alias. Runtime selection is owned by the
compiled catalog; support packages additionally expose the canonical
`catalog_detection` result.

## Deferred Work

- Downloadable online catalog bundles;
- signed catalog updates;
- automatic publication of user-generated descriptors;
- dynamically downloaded executable parsers or transports.

## Acceptance Criteria

- Every supported protocol is represented by generic probe actions.
- Every supported device or family fallback has a descriptor.
- Every runtime surface is selected declaratively.
- Detection drivers contain transport execution and decoding, not model rules.
- Ambiguity is explicit and never resolved by catalog order.
- Support packages explain candidates, evidence, decision path, and surface.
- No normal runtime path requires SmartESS access.
- Startup entity reconciliation uses a confirmed or valid persisted surface.
