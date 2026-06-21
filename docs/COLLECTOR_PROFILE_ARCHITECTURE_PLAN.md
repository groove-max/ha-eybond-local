# Collector Profile Architecture Plan

This is a maintainer plan for the collector-side architecture. It covers three
related problems that should be solved together instead of with separate
heuristics:

- cloud endpoint formatting and restore safety;
- collector kind / capability handling, including ESP EyeBond Collector;
- future callback session identity binding for routed, VPN, and NAT setups.

The current implementation already has an early collector cloud profile catalog
at `custom_components/eybond_local/protocol_catalogs/collector_cloud_profiles.json`.
This plan extends that idea into the source of truth for collector-cloud behavior.

## Problem statement

The integration currently treats the collector callback endpoint mostly as a
string. That is not enough.

Known collector cloud families use different endpoint shapes:

| Family | Default host | Default port | `CLDSRVHOST1` shape | Session style |
|---|---:|---:|---|---|
| `legacy_binary` | `ess.eybond.com` | `502` | host only, for example `ess.eybond.com` | framed EyeBond binary |
| `smartess_at` | `dtu_ess.eybond.com` | `18899` | `host,port,protocol`, for example `dtu_ess.eybond.com,18899,TCP` | text AT |
| `smartvalue_at` | `m2m.eybond.com` | unknown / catalog-defined later | profile-defined | likely SmartESS-like, not confirmed here |

Formatting the endpoint generically can break restore. A known regression class:
legacy collectors that originally stored `ess.eybond.com` must be restored as
`ess.eybond.com`, not as `ess.eybond.com,502,TCP`.

There is a second issue: when Home Assistant switches a collector to HA-only, it
physically overwrites `CLDSRVHOST1`. If the original cloud endpoint is not saved
before that write, deleting and re-adding the integration can permanently lose
the true cloud endpoint.

There is a third issue: remote/VPN/NAT setups can make many collectors connect
back from the same router address. Source IP is not a stable collector identity.
Future routing must bind TCP sessions by collector identity, not by peer IP.

## Target abstractions

### Collector cloud profile

`CollectorCloudProfile` describes cloud-facing behavior of a factory collector
family:

- provider, for example `smartess` or `smartvalue`;
- profile key / family key, for example `legacy_binary`, `smartess_at`;
- known hosts and ports;
- default host, port, protocol;
- `CLDSRVHOST1` write format;
- local HA endpoint write format;
- restore endpoint format;
- session protocol family;
- session identity strategy.

Example target shape:

```json
{
  "family": "legacy_binary",
  "provider": "smartess",
  "label": "SmartESS legacy ESS",
  "default_host": "ess.eybond.com",
  "default_port": 502,
  "default_protocol": "TCP",
  "known_hosts": ["ess.eybond.com"],
  "known_ports": [502],
  "endpoint_write_format": "host_only",
  "session_protocol": "eybond_framed",
  "identity_strategy": "framed_heartbeat_then_fc2_pn"
}
```

```json
{
  "family": "smartess_at",
  "provider": "smartess",
  "label": "SmartESS DTU ESS",
  "default_host": "dtu_ess.eybond.com",
  "default_port": 18899,
  "default_protocol": "TCP",
  "known_hosts": ["dtu_ess.eybond.com"],
  "known_ports": [18899, 38899],
  "endpoint_write_format": "host_port_protocol",
  "session_protocol": "at_text",
  "identity_strategy": "at_dtupn"
}
```

### Collector kind / capability profile

`CollectorKindProfile` describes what the collector itself can do, regardless of
which cloud family it talks to.

Factory collectors usually combine:

```text
collector_kind = factory_eybond
cloud_profile = legacy_binary | smartess_at | smartvalue_at | unknown
```

The community ESP bridge is different:

```text
collector_kind = esp_eybond_bridge
cloud_profile = local_only / none
```

The ESP bridge should be represented through collector capabilities instead of
scattered UI checks:

- virtual bridge: true;
- allowed operation modes: HA-only only;
- proxy capture: false;
- cloud evidence: false;
- Wi-Fi management: true;
- UART runtime speed change: true on ESP8266/ESP32, false on BK72xx/LibreTiny;
- identity probe: `AT+VDTU`.

## Session identity

The primary session identity should be collector PN / DTU PN.

Proxy captures confirm that both known SmartESS families expose PN early:

- `smartess_at` / `dtu_ess.eybond.com`: server asks `AT+DTUPN?`; collector
  replies with `AT+DTUPN:<collector_pn>`.
- `legacy_binary` / `ess.eybond.com`: server sends framed heartbeat; collector
  replies with heartbeat payload containing the collector PN. The first
  `FC_QUERY_COLLECTOR` response also returns parameter `2` with the same PN.

Session ownership must therefore move toward:

```text
TCP session -> collector_pn -> config entry
```

not:

```text
TCP peer IP -> config entry
```

This is required for multi-collector routed/NAT setups, where all callbacks may
arrive from the same router IP.

## Endpoint preservation rules

Before any write to `CLDSRVHOST1`, the integration must preserve the original
cloud endpoint if it can prove it is a cloud endpoint.

Store at least:

- `collector_original_endpoint_raw`;
- `collector_original_endpoint_profile_key`;
- `collector_original_endpoint_observed_at`;
- `collector_original_endpoint_source`;
- collector PN when known.

Safety rules:

- Never overwrite a known-good original endpoint with a local Home Assistant
  endpoint.
- Never infer the original cloud endpoint from the current endpoint if the
  current endpoint points to Home Assistant.
- Restore should prefer the exact raw original endpoint over reconstructed
  formatting.
- If the original endpoint is unknown, do not invent one silently. Show that
  cloud restore is unavailable until the user provides the original endpoint or
  a known profile default is explicitly selected.

Config entry storage alone is not enough. A future persistent collector registry
should store original endpoint facts by collector PN so that deleting and
re-adding the integration does not lose them:

```text
.storage/eybond_local.collectors
collector_pn -> original_endpoint_raw
collector_pn -> cloud_profile_key
collector_pn -> last_seen_ip
collector_pn -> observed_at
```

## Implementation phases

### Phase 1 — expand the collector cloud profile catalog — implemented

- Extend `collector_cloud_profiles.json` with provider, labels, endpoint write
  shape, default port/protocol, session protocol, and identity strategy.
- Add schema/validation tests for required fields and known enum values.
- Keep compatibility functions such as resolving family by host/port.

### Phase 2 — profile-aware endpoint formatting — implemented

- Route all endpoint formatting through the profile catalog.
- Add tests:
  - `legacy_binary` restore writes `ess.eybond.com`;
  - `smartess_at` restore writes `dtu_ess.eybond.com,18899,TCP`;
  - local HA endpoint for `legacy_binary` uses host-only shape;
  - local HA endpoint for `smartess_at` uses `host,port,TCP`;
  - unknown profile preserves exact raw endpoint or fails safe.

### Phase 3 — sticky original endpoint preservation — implemented

- Save original cloud endpoint before HA-only/proxy writes.
- Do not replace saved original with HA/local endpoints.
- Add a persistent collector registry keyed by collector PN.
- Surface “original endpoint unknown” clearly in runtime/support diagnostics.

### Phase 4 — collector kind / capability profile — implemented

- Add a collector-kind profile layer.
- Move ESP bridge special cases into capabilities:
  - HA-only only;
  - no cloud evidence;
  - no cloud proxy capture;
  - Wi-Fi management available;
  - UART runtime switching depends on hardware (`BK72xx/RTL87xx/LibreTiny`
    disables it).
- Keep factory collector behavior unchanged by default.

### Phase 5 — passive session inventory — implemented

- Stop using remote IP as the only pending-session key internally.
- Track incoming sessions by session id plus peer IP/port as diagnostics.
- Add support archive fields:
  - pending session count;
  - duplicate peer IP count;
  - first bytes / protocol shape, anonymized.

### Phase 6 — session classifier — implemented

- Implement passive classifiers:
  - AT text: detect `AT+DTUPN` response when present;
  - framed: detect heartbeat PN and `FC_QUERY_COLLECTOR` parameter `2`.
- Implement active classifiers only for safe modes:
  - `smartess_at`: `AT+DTUPN?`;
  - `legacy_binary`: framed heartbeat and/or `FC=2` parameter `2`.
- Do not inject active probes into an already-owned proxy/cloud stream unless
  the mode explicitly allows it.

### Phase 7 — routing by collector identity — implemented

- Prefer `collector_pn -> entry` binding when PN is known.
- Keep IP-based routing for normal LAN legacy entries.
- For unresolved identity, keep diagnostics available but do not expose controls
  or send inverter commands.
- Add tests for:
  - two collectors behind the same NAT IP;
  - Home Assistant restart causing many simultaneous callbacks;
  - one unresolved session not replacing an already-owned session.

### Phase 8 — proxy and shadow-learning route identity — implemented

- Proxy capture and shadow-learning routes receive the same collector PN and
  session-protocol context as the runtime transport.
- A proxy route no longer has to claim a pending callback solely by peer IP. If
  a collector PN is known, the route first tries to match an already-classified
  pending session, then performs the profile-specific safe identity probe when
  necessary.
- Passive bytes consumed during route identity selection are replayed into the
  proxy handler, so traces do not lose the beginning of the collector stream.
- Session-id maps are cleaned up together with PN indexes when a collector
  connection is released.

## Remaining work

- Validate the PN-routed callback behavior on a real multi-collector setup,
  especially during Home Assistant restart and route transitions.
- Treat `smartvalue_at` as a catalog/profile candidate until real traffic
  evidence confirms its exact callback behavior.

## Non-goals for the first iteration

- Do not add a heuristic that maps any private gateway callback to a configured
  collector IP.
- Do not silently reconstruct unknown original cloud endpoints.
- Do not make SmartValue assumptions beyond the catalog/profile layer until
  traffic evidence confirms its exact session behavior.
