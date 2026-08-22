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
| DESSMonitor API | Read-only metadata collection | Never changed | None | Device metadata and redacted support evidence |

DESSMonitor is intentionally not an alternate implementation of active shadow
learning. It can expose a broader field catalog, current values, setting names,
and a digest of the latest raw packet, but those facts do not prove a local
register mapping. Consequently its result cannot create an entity, activate a
control, or write a device-scoped overlay.

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

## Load-bearing checks

- `test_cloud_learning_engines.py` checks strict models, exact source resolution,
  compatibility, and one declared default.
- `test_dessmonitor_cloud.py` checks signing, exact-PN binding, read-only actions,
  bounded evidence, and credential non-disclosure.
- `test_dessmonitor_learning.py` checks that the metadata runner never opens a
  route or invokes a learning writer.
- `test_config_flow.py` checks source UX, consent separation, transient state,
  metadata review, and zero endpoint cleanup on metadata-only failure/cancel.
- `test_cloud_evidence_architecture.py` keeps provider resolution separate from
  learning-engine execution.
