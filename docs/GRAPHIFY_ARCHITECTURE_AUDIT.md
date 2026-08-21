# Graphify architecture audit

This decision record explains how Graphify metrics are interpreted in this
repository and where the decomposition boundary currently ends. It deliberately
does not equate file length or raw graph degree with an architecture defect.

Current WIP graph (2026-08-20): 9,501 nodes, 22,879 edges and 321 communities.
The graph was rebuilt after the config/options/runtime/transport/integration/
recovery decomposition described below.

## Interpreting God Nodes

Graphify represents a class and each method as adjacent nodes, and also includes
tests in the graph. Raw degree therefore combines:

1. methods contained by the class;
2. test references;
3. real cross-module production consumers.

Only the third category is evidence of production coupling. High degree is a
reason to inspect ownership, not an instruction to split a class. Shared typed
vocabulary such as `WriteCapability`, `OnboardingResult`, `RuntimeSnapshot` and
`RecoveryContract` is expected to have broad fan-in. Stateful authorities such
as `CallbackSessionRegistry` and `_SharedEybondListener` must stay singular even
when their degree is high.

## Completed composition-root decomposition

The former large orchestration roots are now small composition modules. Each
split has a load-bearing architecture guard that preserves the original method
or definition multiset exactly once and rejects backward imports or duplicate
state owners.

| Former root | Current root | Concrete ownership modules |
| --- | ---: | --- |
| `config_flow.py` | 41 lines | journey-specific `config_*.py` mixins and neutral form/presentation helpers |
| `options_flow.py` | 28 lines | `options_*.py` lifecycle/journey mixins |
| `runtime/coordinator/` | lazy public package + 335-line root | 19 cohesive coordinator responsibility mixins |
| `runtime/hub/` | 165-line package root | lifecycle, refresh, management, support, detection and snapshot mixins |
| `runtime/link/` | 175-line package root | session projection, callback, cloud routes, connection, transport lifecycle and one wire-authority mixin |
| `collector/transport.py` | 39 lines | common framing, socket connections, one shared listener, proxy route and framed/AT facades |
| package `__init__.py` | 379 lines | HA lifecycle root plus registration, metadata, entity, precision and migration modules |
| `connection/recovery/verification.py` | 102 lines | immutable models, one reset engine, one observed-session channel and one production transaction assembly |

The split is structural, not semantic:

- config and options still expose one Home Assistant flow-manager lifecycle;
- `EybondLocalCoordinator`, `EybondHub` and `EybondRuntimeLinkManager` still
  each have one object identity and one constructor-owned state set;
- `_SharedEybondListener`, `_LISTENERS` and its session inventory exist in one
  module only;
- `_ControlledResetRecoveryEngine` remains the only recovery state machine;
- all public facade exports are the exact concrete types, never wrappers or
  parallel models.

The corresponding guards are:

- `test_flow_module_boundaries.py`
- `test_coordinator_module_boundaries.py`
- `test_hub_module_boundaries.py`
- `test_link_module_boundaries.py`
- `test_transport_module_boundaries.py`
- `test_integration_module_boundaries.py`
- `test_recovery_verification_module_boundaries.py`

## Post-decomposition compatibility cleanup

After the structural split, a second audit distinguished persisted compatibility
from internal Python shims. Persisted entry migration, legacy endpoint formats,
cloud protocol families and typed-telemetry fallback views remain supported.
The following internal-only surfaces were removed because production no longer
called them:

- `runtime.poll_policy` now has no re-export shim; the scheduler imports the
  neutral top-level polling contract directly;
- negotiated session consumers use `wire_framing`, role-specific adapter ids and
  `negotiate_wire_result`; the old `wire` property, primary-source projection,
  generic adapter aliases and `negotiate_wire` wrapper are gone;
- runtime link tests use the authoritative inverter-adapter selection directly,
  so `_uses_at_text_payload` is no longer a parallel boolean projection;
- payload exchange requires `async_send_payload(..., route=...)`; it no longer
  silently adapts a legacy `async_send_forward`-only object;
- cloud-evidence availability and export use the provider-neutral coordinator
  surface, without SmartESS-named availability/export wrappers;
- the unused coordinator `device_info` alias is gone; entity code names the
  collector/inverter owner explicitly;
- test-only connection-policy simulation and declaration helpers live in tests,
  not in the production policy module.

Static inspection of all 282 production modules reports zero top-level runtime
import cycles and zero implementation-to-facade back-edges across the decomposed
families. Boundary and behavior tests make these removals load-bearing.

## Authorities intentionally kept whole

### CallbackSessionRegistry

The registry owns two views over one state:

- read-only listener observations;
- mutable identity/session claims, certification and handoff.

Claim and handoff decisions must be checked against the exact current session
under the same authority. Splitting these into independent registries would add
a synchronization/TOCTOU defect. Read-only facades are acceptable only as views
over the same object; claims, observations and reconciliation must never be
duplicated.

### Shared listener/session inventory

`collector/transport/listener.py` is still large because it owns one TCP
listener, pending sockets, exact-session indexes, inventory state, route
reservations and activation. These fields participate in the same atomic
selection decisions. The surrounding framing connections, proxy route and
client facades were extracted, but the listener itself remains one class and
one registry.

### Proxy and shadow-learning lifecycle

`CoordinatorCloudToolsMixin` remains cohesive. Proxy capture and shadow
learning share one endpoint-operation authority, cancellation-safe persistence,
restore and terminalization rules. Extracting isolated helpers while leaving
their mutable state on the coordinator would obscure rather than improve the
state machine. A future extraction is justified only if the entire lifecycle —
state, token, persistence, restore and typed outcomes — moves as one authority.

### Admission and discovery

`CollectorAdmissionTransaction` is one callback/inbound admission lifecycle;
`PassiveCallbackDiscovery` is one domain singleton over the shared listener and
registry. Both already delegate wire and recovery work to lower authorities.
Their size is internal state-machine density, not duplicated ownership.

### Protocol drivers and BLE backend

The large SMG and EyeBond-G ASCII modules contain protocol-specific reads,
decoders and derived states. `smartess_ble.py` contains the BLE parser plus its
scanner/link/session/provisioner backend. They do not act as cross-layer
composition roots and do not own registry, recovery or endpoint-operation
state. They may be split later for protocol-maintenance ergonomics, but only
around a typed protocol contract and with no second driver or BLE session
authority. File length alone is not a reason.

## Real back-edges removed

The original audit found `connection/branch_registry.py` importing upper-layer
runtime and onboarding implementations. Construction now lives in
`runtime/factory.py` and `onboarding/factory.py`; the neutral branch registry
keeps only metadata/spec validation. The adjacent `models ↔ branch_registry`
cycle was removed by moving branch-aware builders into
`connection/spec_factory.py`.

The `collector_endpoint ↔ collector.cloud_family` cycle was removed by moving
the high-level callback formatter into `collector/callback_endpoint.py`.

The newer decompositions also removed implicit root dependencies:

- runtime polling imports the exact sensor-precision reconciliation module,
  not the package root;
- recovery implementation modules import their lower-level model/channel
  owners, never the recovery facade back;
- transport implementation modules import the shared-listener owner directly,
  never `collector.transport` back;
- integration helpers import no package-root implementation.

## Validation state

At this checkpoint:

- quality gate: 5/5 (full unit discovery included);
- HA 2026.7 / Python 3.14 lane: 57/57;
- HA 2026.2 / Python 3.13 lane: 57/57;
- `py_compile` and `git diff --check`: clean.

The HA lanes must run sequentially: both exercise a real shared listener and can
otherwise compete for the fixed test port.

## Completion criterion and future work

The architecture decomposition is complete at the ownership boundary: no large
composition root still combines unrelated authorities. Further splitting is
appropriate only when at least one of these is true:

1. Graphify plus source inspection shows an unwanted production dependency;
2. two independently changing lifecycles share a module but not state;
3. a complete state machine can move behind an existing typed contract;
4. a concrete defect demonstrates that the current boundary hides ownership.

Do not split singular registries/listeners/transactions merely to reduce line
count or raw degree. Treat `EXTRACTED` graph edges as navigation hints, exclude
test and same-file containment edges before judging coupling, and never use an
`INFERRED` documentation edge as evidence for a code change.
