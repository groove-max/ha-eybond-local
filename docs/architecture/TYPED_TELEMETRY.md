# Typed telemetry architecture

EyeBond Local historically exposes one broad `RuntimeSnapshot.values` mapping.
That mapping is intentionally flexible, but it mixes live inverter measurements,
collector metadata, runtime status, support diagnostics, and structured values.
Consumers therefore cannot tell from the mapping alone whether a key is a
measurement, whether a value was read in the current cycle, or whether it was
carried from an earlier partial read.

The typed telemetry architecture provides a narrow measurement contract while
keeping non-measurement runtime metadata in its existing typed owners and
diagnostic projections.

## Current foundation

`telemetry.py` owns the neutral immutable model:

- `TelemetryPoint` accepts only exact scalar values and records their exact
  scalar kind and freshness;
- `TypedTelemetryFrame` contains unique points for one exact driver;
- `fold_driver_telemetry()` applies the existing driver `FULL`, `DELTA`, and
  explicit-removal semantics;
- structured diagnostics remain in `RuntimeSnapshot.values` and do not enter
  the typed frame.

The runtime hub folds a frame at the same boundary that already owns the
last-good driver-value cache. A disconnected or failed snapshot retains the
last values but marks every typed point as carried. `RuntimeSnapshot.telemetry`
publishes the frame beside `RuntimeSnapshot.values`.

`DriverReadResult` is the strict driver-to-runtime envelope. Its direct
constructor rejects malformed field types, unnormalized keys, contradictory
FULL/removal semantics, and overlapping measurement/diagnostic ownership. The
three production call sites that invoke `async_read_values()` all pass the
result through the single `coerce_driver_read_result()` compatibility boundary;
an exact legacy `dict` still means FULL, while duck-typed results fail closed.

This foundation is deliberately not a second runtime authority. Driver reads
remain authoritative. The hub now publishes driver scalar values only through
the typed frame; `RuntimeSnapshot.values` retains metadata and structured
diagnostics, while `runtime_values()` constructs an explicit compatibility view
for mapping consumers.

## Completed migration

1. Project driver-produced scalar values into the typed frame. **Implemented.**
2. Add typed provenance for canonical/derived measurements while retaining the
   existing canonical compatibility aliases. **Implemented.**
3. Move sensor and binary-sensor reads to typed points where coverage is proven;
   keep an explicit compatibility path for metadata and diagnostics.
   **Implemented for all Home Assistant measurement consumers.**
   Derived energy, summary attributes, display-precision repair, capability
   entities, write validation, clock tooling, and support UI schema use the same
   typed-first compatibility view. Lifecycle/tooling metadata stays in the
   legacy source until it has an explicit typed model.
4. Remove the stored broad-map measurement copy only after parity tests prove
   that every supported driver and learned overlay has typed coverage.
   **Implemented.** Mapping-oriented callers use the non-stored
   `runtime_values()` compatibility view.

## Current mapping boundary

`RuntimeSnapshot.values` no longer contains the hub's compatibility copy of
driver scalar values. The driver read, last-good cache, and
`TypedTelemetryFrame` are resolved at one hub boundary. The broad mapping keeps
metadata, structured diagnostics, support artifacts, and lifecycle state;
`RuntimeSnapshot.runtime_values()` merges both views for the remaining mapping
consumers without creating a second stored measurement authority.

Phase 4 removed that copy after all of the following became true:

- every built-in driver declares FULL or DELTA explicitly instead of relying on
  the exact-dict FULL adapter; **implemented** (`FULL`: SMG, SRNE, MUST, catalog
  Modbus; `DELTA`: PI30, PI18, G-ASCII, SmartESS 0925);
- learned/device-scoped overlays have parity coverage for their exposed scalar
  values; **implemented** (an activated learned SMG schema is read through the
  production driver and folded into a fresh typed point);
- support bundles and fixture tooling intentionally choose typed measurements
  versus broad metadata rather than depending on an undifferentiated mapping;
  **implemented** (`runtime.telemetry` and `runtime.metadata` are separate
  support payloads, while fixture replay preserves `DriverReadResult` mode,
  removals, and diagnostics through its tooling boundary);
- an architecture test proves no Home Assistant measurement consumer reads the
  broad mapping directly.

## Invariants

- No coercion at the typed-model boundary: `bool`, `int`, `float`, and `str`
  remain distinct; non-finite numbers and structured objects are rejected.
- `FULL` replaces a driver's prior frame. `DELTA` carries untouched points and
  removes only explicit invalidations.
- A driver identity change cannot carry telemetry from the previous driver.
- An error or disconnected snapshot cannot describe retained data as fresh.
- Metadata, tooling state, lists, and dictionaries are not silently classified
  as measurements.
- Rotating writable-setting caches are non-persisted driver runtime state. They
  are folded into the compatibility values needed by capability entities, but
  never stored in `DetectedInverter.details`, promoted to identity evidence, or
  exported as an opaque internal cache in a Support Archive.
- Driver diagnostics have an exact per-identity replacement lifecycle in the
  hub. They remain in the broad compatibility snapshot, never enter the typed
  measurement frame, and disappear when omitted by the next successful result
  or when the selected inverter identity changes.
- The typed layer must not decide connection, recovery, ownership, or driver
  selection.

## Adjacent collector metadata boundary

Collector connection metadata does not belong in telemetry. The existing
`CollectorInfo` model remains its typed owner; creating a second generic
`RuntimeMetadata` frame would only duplicate authority.
`RuntimeSnapshot.set_collector_server_endpoint()` synchronizes the typed
collector endpoint with the broad `values` projection atomically. Reads prefer
`CollectorInfo` and fall back only for old or partially constructed snapshots.
Endpoint validation, persistence, wire writes, and recovery remain owned by
their existing authorities.

The broad mapping is therefore not unfinished typed telemetry work. It is the
intentional projection surface for heterogeneous lifecycle state, structured
diagnostics, support artifacts, and metadata whose typed owners already live
elsewhere. A repository guard prevents every measurement entity platform from
reading that mapping directly; collector endpoint text remains the sole entity
exception because it exposes metadata rather than telemetry.
