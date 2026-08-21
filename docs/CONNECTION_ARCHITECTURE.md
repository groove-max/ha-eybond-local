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
| `proxy_enabled` | bool | Retired compatibility field; new options writes keep it disabled. |

### `connection_strategy`

- **`inbound`** — the collector connects to Home Assistant on its own (an ESP
  bridge, or a collector whose endpoint was already pointed at Home Assistant).
  Runtime **never** sends a UDP callback trigger; it claims or waits for the
  inbound session.
- **`callback_on_demand`** — Home Assistant asks the collector to dial back.
  Runtime sends **exactly one** UDP trigger per connect attempt (see below).

ESP EyeBond Collector persists a valid discovery redirect as its server endpoint.
The options form may prefill the bridge's current management-readback endpoint,
but that value is presentation context only: it is not a recovery proof and does
not bypass the verified inbound transition.
For that capability profile, callback is therefore a bootstrap route rather than
a stable operating profile. Its normal options surface manages the Home Assistant
endpoint and runs either callback-origin → inbound verification or a verified
inbound → inbound endpoint relocation through this same transition authority.
It never exposes a cloud rollback chooser.

#### `entry.data` is the single canonical owner (schema v4)

`connection_strategy` has **exactly one** owner: **`entry.data`**. Every authority
that can change it writes there:

| Authority | Writes |
|---|---|
| Config flow onboarding (verified) | `data` |
| Options form ("how the collector connects") | `data`, via one atomic `async_update_entry(data=…, options=…)` |
| HA-only / Cloud+HA operation-mode action | `data` (`_persist_connection_axes`) |
| Bind to HA / rollback endpoint | `data` |

`entry.options` **must never hold an active copy.** Before v4 it did: the options
form wrote the strategy into options while the endpoint actions wrote it into
data, and the resolver read options first — so a stale options value silently
shadowed a successful Cloud+HA / HA-only switch and the user had to re-pick the
strategy by hand. `resolve_connection_strategy` now reads **data first** and falls
back to options **only** as a pre-migration legacy source;
`_connection_strategy_source` reports `explicit_data` vs
`legacy_options_pre_migration` so a support bundle shows which one was used.

The options form commits data+options in a **single** `async_update_entry`, so the
entry reloads exactly once and only after the state is consistent (the terminal
`async_create_entry(data=options)` then writes an unchanged value, which HA
ignores without firing a second listener).

`endpoint_control_policy` and the endpoint write-provenance
(`endpoint_written_value`/`at`) live in `entry.data` for the same reason.

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

This field is retained only so older entries migrate without losing schema
compatibility. The former options toggle promised continuous HA + cloud
forwarding, but no runtime consumer implemented that behavior, so the toggle is
no longer exposed and new options writes normalize the field to `false`.

Temporary **Collector traffic capture** is a separate, explicit diagnostics
action with its own route lease and timer; it is not controlled by this field.

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

## Controlled reset and overlapping sessions

A callback-mode entry is legitimately idle between polls. Before a
`callback_on_demand` → `inbound` transition can write the persistent Home
Assistant endpoint, the coordinator therefore asks the existing runtime
connection path to establish one management session. This is not a second
recovery implementation: it uses the normal one-shot callback trigger and
causality lease, then pins the registry's exact currently-observed socket. The
neutral transition authority still refuses without that session and still owns
the active identity/restart/reconnect proof. The whole bootstrap and transition
hold the coordinator's runtime-operation lock, so ordinary polling cannot race
their wire traffic.

A strategy transition sends its endpoint write/apply or reboot through one
exact, registry-owned physical session. The temporary management facade is
session-pinned and its teardown preserves that socket; it cannot switch to a
same-PN sibling and cannot create reconnect evidence by closing the socket
itself.

Reset activity is observed against the complete pre-action session cohort, not
against one selected socket's EOF. Some collectors briefly keep several
same-PN sockets alive: a baseline sibling may close, or a new socket may open,
while the management socket survives. This activity only opens the recovery
wait. A proof still requires a new session id outside the whole baseline, a
strong exact PN, the expected listener route, and a successful same-owner
registry retarget. Baseline sessions and foreign-PN sockets can never certify a
transition.

## Endpoint ownership rules

- The integration never silently redirects a collector.
- Pointing a collector at Home Assistant (bind) and restoring its previous
  endpoint (rollback) are **explicit, reversible actions**.
- **Explicit rollback is the only restore path** for a previous endpoint. It
  restores the remembered endpoint and flips `endpoint_control_policy` back to
  `external`.
- `integration_managed` implies the integration wrote the endpoint and recorded
  provenance (`endpoint_written_value`/`at`).

Endpoint serialization and route semantics are separate facts. The declarative
collector cloud-profile catalog is the single authority for both:

- `host_port_protocol` and `host_port` collectors expose an editable advertised
  port while preserving their required wire shape.
- `host_only` collectors store only the host, but the catalog default port is
  still part of the route. For the legacy binary family this is TCP `502`.
  When configuring the persistent HA-only endpoint, transition UI therefore
  does not ask for a port the firmware cannot store; listener preparation,
  reconnect matching and persisted `advertised_tcp_port` all use `502` while
  `endpoint_written_value` remains host-only.
- A `set>server=host:port` callback route is a separate transport fact. It does
  not inherit the persistent endpoint's host-only shape or implicit port. Its
  advertised port is explicit (and may be an external NAT/VPN forwarding port),
  while `listener_port` remains the local configured callback listener. The
  integration passively listens on the conventional `502`, `8899` and `18899`
  ports, but those listener defaults are not a global allowlist for advertised
  callback ports.
- A pre-existing host-only record with generic-port metadata is never rewritten
  as a migration guess. Selecting the current HA-only profile routes through the
  normal restart/same-PN verification and commits the corrected route only after
  the collector reconnects on the fixed listener.

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

`async_migrate_entry` in `__init__.py`; new entries are stamped at creation by the
config flow (`EybondLocalConfigFlow.VERSION`).

| Step | What it does |
|---|---|
| 1 → 2 | Derives the axes from legacy fields via `migrate_entry_axes` (hostname-free). |
| 2 → 3 | Corrective re-migration of the provably-unreachable inbound **cloud-primary** shape only (never manual/known-IP). |
| 3 → 4 | Makes `entry.data` the canonical owner of `connection_strategy`: freezes the entry's real **pre-upgrade effective** value into data and **deletes the options copy**. |

The v4 step uses `legacy_effective_connection_strategy()`, which reproduces the
OLD **options-first** resolution exactly. That is deliberate: when data and options
disagree, the entry really *behaved* as the options value said, so migration
preserves that actual behavior instead of "healing" the conflict toward data. The
frozen value is never re-derived from hostname, endpoint, cloud provider,
collector kind or peer IP.

After v4, `entry.options` is consulted for the strategy **only** as a legacy
fallback for an entry that has not migrated yet.
