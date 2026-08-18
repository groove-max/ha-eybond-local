# Graphify architecture audit

This audit records how Graphify's structural metrics should be interpreted for
this repository.  It is intentionally a decision record, not a request to split
large classes merely because they have a high raw degree.

Baseline: `c8b577f` (`Publish driver telemetry outside broad runtime values`).
The incremental code graph contained 9,412 nodes and 22,179 edges; reclustering
produced 359 communities.

## Why raw God Node degree is misleading here

Graphify represents a class and each of its methods as adjacent nodes.  It also
includes invariant and lifecycle tests in the same graph.  Consequently, raw
degree combines three different things:

1. methods contained by the class;
2. test references to the class;
3. actual consumers in another production module.

Only the third category is evidence of cross-module coupling.  Recounting the
four most prominent orchestration/authority classes after excluding same-file
and test edges gives:

| Node | Raw degree | Same-file edges | Test edges | External production edges | External production files |
| --- | ---: | ---: | ---: | ---: | ---: |
| `CallbackSessionRegistry` | 159 | 36 | 112 | 9 | 2 |
| `EybondLocalCoordinator` | 309 | 280 | 0 | 27 | 8 |
| `EybondLocalConfigFlow` | 224 | 218 | 2 | 0 | 0 |
| `EybondLocalOptionsFlow` | 177 | 172 | 0 | 0 | 0 |

The counts do not always sum to raw degree because generated/container edges
without a production source file are deliberately left unclassified.

The adjusted class ranking is led by shared contracts and extension points such
as `WriteCapability` and `InverterDriver`, not by the session registry.  Their
high external degree is expected: they are the vocabulary shared by drivers,
runtime orchestration, and Home Assistant entities.

## CallbackSessionRegistry decision

The registry has two logical surfaces:

- a read-only projection of listener observations;
- mutable collector identity, session claim, certification, and handoff state.

They intentionally share one state owner.  Claim and handoff decisions must be
checked against the current exact-session observation under the same authority.
Splitting them into independent registries would create a synchronization and
TOCTOU problem rather than remove one.

Production mutation is concentrated in the connection transactions/recovery
authorities, config-entry setup/removal, and the explicit runtime pinning path.
Passive discovery supplies observations and consumes unclaimed projections; it
does not own a second claim table.  The remaining config-flow calls are adapters
for the legacy manual continuation and pending-entry compatibility path, while
admission-origin flows use `CollectorAdmissionTransaction`.

Decision: **keep one registry state owner**.  Narrow read-only or ownership
facades may be introduced later to make call-site permissions clearer, but only
as views over the same object and only when they remove a demonstrated unwanted
dependency.  They must never duplicate claims, observations, or reconciliation.

## Coordinator decision

The coordinator is a large Home Assistant orchestration root, but its external
production consumers are the integration setup and entity platforms.  Most of
its raw degree is its own methods.  It already delegates important authority to
the runtime link manager, endpoint-operation authority, transition/recovery
modules, telemetry frame, and support tooling.

The remaining risk is lifecycle density inside the class, not external fan-in.
Safe future extraction candidates are cohesive state machines with an already
typed boundary and load-bearing tests, for example one complete proxy-capture or
shadow-learning lifecycle.  Pure helper extraction that leaves the same mutable
state spread across two owners is not an improvement.

Decision: do not perform a broad coordinator split.  Extract only a complete
lifecycle authority when its state, cancellation boundary, persistence, and
terminal outcomes can move together.

## Config-flow decision

The config and options flow classes are large, but Graphify found no external
production class consumers.  Their degree is almost entirely the flow steps and
helpers contained in `config_flow.py`.  This is a maintainability concern, not a
cross-layer authority defect.

Safe extraction must follow a complete user journey rather than move isolated
formatters.  Candidate journeys include shadow-learning review or strategy
transition repair, but only after defining the flow state passed across the
boundary.  The admission and callback-continuation transactions are examples of
the desired pattern: typed lifecycle ownership first, UI adapter second.

Decision: no file-size-driven rewrite.  Preserve one flow-manager lifecycle and
extract only a journey with a typed state/terminal contract.

## How to use Graphify findings

- Treat `EXTRACTED` call/import edges as navigation hints, then verify callers in
  source.
- Exclude same-file method/container edges and tests before calling something a
  production bridge.
- Do not use an `INFERRED` documentation edge as evidence for a code change.
- Prefer directed call/import analysis for authority questions; the aggregated
  undirected visualization cannot establish ownership direction.
- A high degree is a reason to inspect a node, not a refactoring requirement.

## Concrete back-edge removed

The adjusted audit found one real dependency-placement defect that raw God Node
ranking did not highlight: `connection/branch_registry.py` imported both
`runtime.hub.EybondHub` and `onboarding.eybond.OnboardingDetector`.  A neutral
connection registry therefore acted as the composition root for two upper
layers.

The registry now contains only connection metadata and typed spec/value
builders.  `runtime/factory.py` owns construction of the runtime implementation,
and `onboarding/factory.py` owns construction of the onboarding implementation.
The branch validation stays shared; no connection or detection algorithm was
duplicated.  An architecture test prevents the upper-layer imports from
returning.

The same pass removed the adjacent `models ↔ branch_registry` import cycle.
Immutable connection models now contain no factory imports; the three
branch-aware builders live in `connection/spec_factory.py`, which is the single
composition point over models and branch metadata.

## Current priority

No runtime trust-boundary defect was found by the God Node audit.  After the
composition-root back-edge above, further extraction should wait for either a
specific lifecycle defect or a measurable unwanted production dependency.
