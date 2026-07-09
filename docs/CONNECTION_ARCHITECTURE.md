# Collector Connection Architecture (maintainer reference)

This is a maintainer-facing reference for how EyeBond Local decides *how* a
collector is connected, *who* owns a live collector session, and *whether* the
integration may touch the collector's server endpoint. It is the contract that
the phase 2–8 refactor established; read it before changing anything in
`connection/`, `runtime/link.py`, `runtime/coordinator.py`,
`collector/transport.py`, or `passive_discovery.py`.

The whole point of this design is that these decisions are **explicit and
opaque**. They are *not* inferred from hostnames, peer IP addresses, or collector
type at runtime.

## Three independent connection axes

Every config entry carries three durable, opaque axes. They are resolved by
`connection/connection_policy.py` (which reads the explicit persisted value, else
derives it from legacy fields exactly as `migrate_entry_axes` does).

| Axis | Values | Meaning |
|---|---|---|
| `connection_strategy` | `inbound` \| `callback_on_demand` | Who dials whom. |
| `endpoint_control_policy` | `external` \| `integration_managed` | May the integration write/restore the collector's server endpoint. |
| `proxy_enabled` | bool | Independent, capability-gated proxy-capture flag. |

### `connection_strategy`

- **`inbound`** — the collector connects to Home Assistant on its own (an ESP
  bridge, or a collector whose endpoint was already pointed at Home Assistant).
  Runtime **never** sends a UDP callback trigger; it claims or waits for the
  inbound session.
- **`callback_on_demand`** — Home Assistant asks the collector to dial back.
  Runtime sends **exactly one** UDP trigger per connect attempt (see below).

The runtime gates on the axis, never on the operation mode / hostname /
collector type: `EybondLocalCoordinator._configure_reverse_discovery_mode` uses
`may_run_steady_reverse_discovery(connection_strategy)`.

### `endpoint_control_policy`

The collector's server endpoint (SmartESS param-21 / `CLDSRVHOST1`) is an
**opaque string**. Do not parse a provider or an ownership claim out of its
hostname.

- **`external`** — the integration **observes** the endpoint but never silently
  writes, restores, or auto-heals it. It may *remember* an observed external
  endpoint (for a later explicit rollback), which is provenance recording, not
  mutation.
- **`integration_managed`** — set **only after** the integration actually wrote
  the endpoint through an explicit user action (bind, or a mode switch that
  wrote). Only then may the per-poll reconcile keep the endpoint aligned to Home
  Assistant.

The per-poll reconcile is `_async_reconcile_collector_operation_mode_endpoint`,
gated by `may_auto_manage_endpoint(endpoint_control_policy)`.

### `proxy_enabled`

Independent of the other two, and only offered when
`collector_capabilities.proxy_capture` is true (community/ESP bridges cannot host
proxy capture, so the toggle is hidden for them — gated in the options flow).

## Session registry owns identity

`connection/session_registry.py` (`CallbackSessionRegistry`) is the single object
that answers "which config entry owns which inbound session". Identity rules:

- **Full collector PN is durable identity.** Two sessions with different full PNs
  are always distinct collectors, even behind one NAT peer IP.
- **Short PN is transient discovery identity only.** A short PN observed from a
  weak source may be a prefix of the full PN read later; the registry *enriches*
  a short-PN claim into the full PN (`reconcile_pn`) rather than creating a
  second owner. Prefix matching requires ≥10 chars (`pn_is_same_identity`).
- **Session id is transient socket identity only.**
- **Peer IP is diagnostic / UDP-target only, never an ownership or dedup key.**
- A session is owned by exactly one entry.

Short/full PN reconciliation lives **only** in the registry (`reconcile_pn` /
`pn_is_same_identity`); the transport, link, and passive discovery delegate to it
instead of re-implementing prefix logic.

The live-wire truth comes from the registry too: `runtime/link.py` reads the
entry-claimed `SessionHandle` (via the runtime-scoped registry over the public
`SharedEybondTransport.observed_collector_sessions()` facade) and negotiates the
wire from the *observed* session, not from a persisted `collector_session_protocol`
hint. Untrusted states (`route_identity_mismatch`, `waiting_for_route_identity`,
`parked_*`, `closed_*`) can never override a claimed, routed session.

## `callback_on_demand` is one-shot

`runtime/link.py` sends exactly one `async_probe_target` datagram per connect
attempt, then bounded-waits for the inbound session. There is **no** continuous
`DiscoveryAnnouncer` loop in the connect path. Typed outcomes are recorded in
`_last_callback_state` and surfaced via `callback_trigger_diagnostics()` into
snapshot values (and the support bundle):

- `callback_connected`
- `callback_timeout`
- `callback_identity_mismatch` (a foreign PN answered; none matched)
- `callback_session_claimed_by_other_entry` (a matching session is owned by a
  different entry)
- `callback_listener_unavailable` / `callback_listener_error`

`inbound` entries send **zero** UDP triggers, even while disconnected.

## Endpoint ownership rules

- The integration never silently redirects a collector.
- Pointing a collector at Home Assistant (bind) and restoring its previous
  endpoint (rollback) are **explicit, reversible actions**.
- **Explicit rollback is the only restore path** for a previous endpoint. It
  restores the remembered endpoint and flips `endpoint_control_policy` back to
  `external`.
- `integration_managed` implies the integration wrote the endpoint and recorded
  provenance (`endpoint_written_value`/`at`).

## Shadow-learning route ownership temporarily blocks reconcile

An active **shadow-learning** route temporarily *owns* the collector's endpoint
as the safety boundary of the scan. While it runs, the per-poll endpoint
reconcile is a **no-op** for **every** policy
(`_async_shadow_learning_owns_endpoint` returns early with
`shadow_learning_active`). This is route ownership, not operation-mode coupling,
and it never writes or restores.

## What must NOT be reintroduced

- **Hostname/endpoint-string ownership heuristics.** Endpoint strings are opaque.
  Framing/protocol *profiles* may use a configured cloud family for byte framing,
  but never to decide ownership.
- **Peer IP as identity or dedup key.** Two collectors behind one public IP must
  stay distinguishable by PN/session. Peer IP is diagnostic / UDP-target only.
- **Collector-type ownership heuristics.** A bridge does not, by itself, define
  the payload session or the endpoint policy.
- **`collector_operation_mode` as a primary runtime driver.** It survives as a
  compatibility/migration/display concept only; runtime branches on the three
  explicit axes.
- **Silent endpoint restore / auto-heal.** Under `external`, the integration must
  not write, restore, or auto-heal the endpoint. Only explicit user actions do.
- **A continuous callback announcer loop** in the connect path. `callback_on_demand`
  is one trigger per attempt.

## Migration

Config entry `VERSION` 1 → 2 (`async_migrate_entry` in `__init__.py`) derives the
three axes from legacy fields via `migrate_entry_axes` (hostname-free). New
entries are stamped at creation in the config flow.
