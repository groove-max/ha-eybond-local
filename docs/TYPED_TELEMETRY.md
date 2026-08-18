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

This foundation is deliberately not a second runtime authority. Driver reads
remain authoritative, and the legacy mapping remains unchanged while consumers
are migrated.

## Migration order

1. Project driver-produced scalar values into the typed frame. **Implemented.**
2. Add typed provenance for canonical/derived measurements while retaining the
   existing canonical compatibility aliases. **Implemented.**
3. Move sensor and binary-sensor reads to typed points where coverage is proven;
   keep an explicit compatibility path for metadata and diagnostics.
   **Implemented for direct value reads.**
   Derived energy, capability entities, write validation, and support UI schema
   use the same typed-first compatibility view; lifecycle/tooling metadata stays
   in the legacy source until it has an explicit typed model.
4. Remove broad-map measurement interpretation only after parity tests prove
   that every supported driver and learned overlay has typed coverage.

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
