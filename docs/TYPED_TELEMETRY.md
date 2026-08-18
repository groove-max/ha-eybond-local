# Typed telemetry migration

EyeBond Local historically exposes one broad `RuntimeSnapshot.values` mapping.
That mapping is intentionally flexible, but it mixes live inverter measurements,
collector metadata, runtime status, support diagnostics, and structured values.
Consumers therefore cannot tell from the mapping alone whether a key is a
measurement, whether a value was read in the current cycle, or whether it was
carried from an earlier partial read.

The typed telemetry migration introduces a narrow measurement contract without
changing the legacy mapping in one large rewrite.

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
remain authoritative, and the legacy mapping remains unchanged while consumers
are migrated.

## Migration order

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
4. Remove broad-map measurement interpretation only after parity tests prove
   that every supported driver and learned overlay has typed coverage.

## Current compatibility boundary

`RuntimeSnapshot.values` still contains a compatibility copy of driver
measurements. This is deliberate and is not a second measurement authority:
the driver read, last-good cache, and `TypedTelemetryFrame` are resolved at one
hub boundary, while the broad mapping preserves older diagnostics, support
artifacts, and callers during migration. Typed points win whenever a migrated
consumer requests the same key.

Phase 4 must not remove that copy until all of the following are true:

- every built-in driver declares FULL or DELTA explicitly instead of relying on
  the exact-dict FULL adapter;
- learned/device-scoped overlays have parity coverage for their exposed scalar
  values;
- support bundles and fixture tooling intentionally choose typed measurements
  versus broad metadata rather than depending on an undifferentiated mapping;
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
- The typed layer must not decide connection, recovery, ownership, or driver
  selection.

## Adjacent collector metadata boundary

Collector connection metadata does not belong in telemetry. The existing
`CollectorInfo` model remains its typed owner; creating a second generic
`RuntimeMetadata` frame would only duplicate authority. During the compatibility
migration, `RuntimeSnapshot.set_collector_server_endpoint()` synchronizes the
typed collector endpoint with the legacy `values` projection atomically. Reads
prefer `CollectorInfo` and fall back only for old or partially constructed
snapshots. Endpoint validation, persistence, wire writes, and recovery remain
owned by their existing authorities.
