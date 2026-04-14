# Remote / NAT Setup Guide

This guide explains when the remote/NAT feature is needed, what problem it solves, and how to configure it without breaking normal local setups.

## What This Feature Is For

EyeBond collectors do not behave like a normal client that only talks outbound to Home Assistant.

The integration announces a callback endpoint in the EyeBond redirect payload:

```text
set>server=<ip>:<port>;
```

The collector then opens a reverse TCP connection back to that endpoint.

That is simple on one local subnet, but it becomes tricky when:

- the collector is on another site
- broadcast discovery does not cross routers
- Home Assistant listens on one local IP, but the collector must call back to a different VPN or public IP
- the externally forwarded TCP port is different from the internal Home Assistant listener port

The new feature splits those two concerns:

- `Local listener IP` / `Local TCP port`: where Home Assistant actually binds and waits for the collector
- `Advertised callback IP` / `Advertised callback TCP port`: what Home Assistant tells the collector to call back to in `set>server=...`

If you leave the advertised fields empty, the integration behaves exactly like the old local-only setup.

## When You Need It

Use remote/NAT fields only if at least one of these is true:

- the collector is not on the same subnet as Home Assistant
- you must use unicast discovery to a known remote collector IP
- the collector reaches Home Assistant through VPN
- the collector reaches Home Assistant through a public IP or DDNS name that is forwarded to the HA host
- the collector must connect to an external TCP port that differs from the local Home Assistant listener port

Do not use these fields for a normal same-LAN installation. In that case, leave them empty.

## Field Reference

| Field | What it means | Typical local value | Typical remote/NAT value |
|---|---|---|---|
| `Local listener IP` | Local IPv4 address Home Assistant binds the TCP listener to | HA LAN IP on the collector subnet | HA LAN IP or VPN IP that exists on the HA host |
| `Collector IP` | Direct IPv4 used for probing and unicast discovery | Empty or collector LAN IP | Public, VPN, or routed IPv4 that reaches the collector |
| `Local TCP port` | TCP port Home Assistant listens on | `8899` | Usually `8899` |
| `Advertised callback IP` | IPv4 written into `set>server=...` | Empty | VPN IP, public IP, or forwarded callback IP |
| `Advertised callback TCP port` | TCP port written into `set>server=...` | Empty | Forwarded external TCP port if different |
| `UDP discovery port` | Collector UDP redirect port | `58899` | Usually `58899` unless your collector uses a custom mapping |
| `Discovery target` | IPv4 that receives discovery redirects | Subnet broadcast like `192.168.1.255` | Specific collector IP for unicast |

## Recommended Scenarios

### 1. Same LAN, same subnet

This is the default case.

- Use auto-discovery if possible.
- If you go through Manual setup, leave `Advertised callback IP` empty.
- Leave `Advertised callback TCP port` empty.
- Keep `Discovery target` as subnet broadcast.

Use the new feature here only if you have a very unusual network.

### 2. Routed VPN between Home Assistant and the collector

Use this when both sides can reach each other over private VPN addresses.

- `Local listener IP`: the Home Assistant VPN IPv4 if Home Assistant binds on that interface
- `Collector IP`: the collector VPN IPv4
- `Discovery target`: usually the same collector VPN IPv4
- `Advertised callback IP`: leave empty if the collector should call back to the same VPN IP that HA is bound to
- `Advertised callback TCP port`: leave empty unless the VPN path rewrites ports

This is usually more reliable than raw public NAT.

### 3. Public IP or DDNS with port forwarding back to Home Assistant

Use this when the collector is remote and must connect to Home Assistant through an externally reachable address.

- `Local listener IP`: the actual local HA IPv4 that exists on the host
- `Local TCP port`: the real local port on the HA host, usually `8899`
- `Collector IP`: the remote collector public IP if you are probing it directly
- `Discovery target`: the same remote collector public IP if you want unicast discovery
- `Advertised callback IP`: the public IP or DDNS-resolved IPv4 that the collector can reach
- `Advertised callback TCP port`: the externally forwarded TCP port if it differs from the local listener port

Example:

- HA host local IP: `192.168.1.50`
- Router forwards public `203.0.113.10:50099` -> `192.168.1.50:8899`
- Collector public IP: `198.51.100.44`

Then configure:

- `Local listener IP` = `192.168.1.50`
- `Local TCP port` = `8899`
- `Collector IP` = `198.51.100.44`
- `Discovery target` = `198.51.100.44`
- `Advertised callback IP` = `203.0.113.10`
- `Advertised callback TCP port` = `50099`

## Step-By-Step In Home Assistant

1. Open `Settings -> Devices & Services -> EyeBond Local`.
2. If the collector is local and discoverable, try auto-scan first.
3. If the collector is remote or auto-scan cannot work across routers, choose `Manual setup`.
4. Enter `Local listener IP` and, if known, `Collector IP`.
5. Open `Advanced connection settings`.
6. Keep `Advertised callback IP` and `Advertised callback TCP port` empty unless the collector must call back through VPN or NAT.
7. For remote setups, change `Discovery target` from broadcast to the exact collector IP you want to probe.
8. Save the entry and wait for the reverse TCP connection.
9. If detection stays partial, create a Support Archive.

## Firewall And Port Notes

- Home Assistant listens for inbound collector TCP on `Local TCP port`, default `8899`.
- The collector receives discovery redirect packets on UDP `58899` unless configured otherwise.
- For remote/NAT setups, the important direction is:

  - UDP from Home Assistant to the collector discovery endpoint
  - TCP from the collector back to the advertised callback IP/port

- If the collector is behind NAT and you use direct public unicast discovery, the collector-side router must forward the UDP discovery port to the collector.
- If Home Assistant is behind NAT and the collector must call back over the internet, the HA-side router must forward the advertised TCP port to the HA host.

## Troubleshooting

### The collector never replies

Check these first:

- `Collector IP` or `Discovery target` points to the wrong address
- UDP `58899` is not being forwarded to the collector
- the collector is reachable only through VPN, not through raw public internet

### The collector replies, but never opens TCP back to Home Assistant

This usually means the callback endpoint is wrong.

Check these first:

- `Advertised callback IP` is not reachable from the collector
- `Advertised callback TCP port` does not match the actual forwarded external TCP port
- the HA-side router is not forwarding inbound TCP to the Home Assistant host

### Local installs stopped working after you edited advanced fields

Reset to the simplest configuration:

- clear `Advertised callback IP`
- clear `Advertised callback TCP port`
- restore `Discovery target` to subnet broadcast
- make sure `Local listener IP` is a real local IPv4 on the Home Assistant host

### It still does not work over the public internet

If either side is behind CGNAT or the routers do not support the needed forwarding cleanly, use VPN instead of raw NAT.

## Safe Default Rule

If you are not sure whether you need this feature, you probably do not.

For a normal local SmartESS / EyeBond setup:

- use auto-discovery
- keep Home Assistant and the collector on the same subnet
- leave all advertised callback fields empty

That remains the recommended path for most users.