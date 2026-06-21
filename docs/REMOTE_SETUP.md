# Remote / NAT Setup Guide

Most users do not need this page.

Use remote setup only when the collector is not on the same local network as Home Assistant, for example through VPN, another router, or port forwarding.

For a normal home network:

- use auto-discovery;
- keep Home Assistant and the collector on the same subnet;
- leave advanced callback fields empty.

## When to use remote setup

Use these fields only if at least one is true:

- the collector is at another site;
- Home Assistant and the collector are connected through VPN;
- auto-discovery cannot cross your router;
- the collector must connect back to Home Assistant through a public IP, DDNS name, or forwarded port;
- you already know the collector IP and need manual setup.

If you are not sure, do not change these fields. Try normal scan first.

## What the fields mean

| Field | Plain meaning | Normal local setup |
|---|---|---|
| **Local listener IP** | The Home Assistant address that should wait for the collector connection. | Home Assistant LAN IP |
| **Collector IP** | The collector address when you already know it. | Empty or collector LAN IP |
| **Local TCP port** | The local Home Assistant port used for the collector connection. | Usually keep default |
| **Advertised callback IP** | The address the collector should use to reach Home Assistant when local address is not enough. | Empty |
| **Advertised callback TCP port** | The external or VPN port the collector should use. | Empty |
| **UDP discovery port** | The discovery port used to wake/probe the collector. | Usually keep default |
| **Discovery target** | Where Home Assistant sends the discovery/probe packet. | Local broadcast or collector IP |

The most common mistake is filling **Advertised callback IP** for a normal local setup. Leave it empty unless the collector really needs a different address to reach Home Assistant.

## Recommended setups

### Same LAN

Use this when Home Assistant and the collector are at the same location.

- Prefer auto-discovery.
- In manual setup, enter the collector IP only if you know it.
- Leave **Advertised callback IP** empty.
- Leave **Advertised callback TCP port** empty.

### VPN

Use this when both sides can reach each other through private VPN addresses.

- **Local listener IP**: Home Assistant VPN IP, if Home Assistant listens on that interface.
- **Collector IP**: collector VPN IP.
- **Discovery target**: usually the collector VPN IP.
- **Advertised callback IP**: leave empty if the collector can reach the local listener IP directly; otherwise use the Home Assistant VPN IP.
- **Advertised callback TCP port**: leave empty unless your VPN or router changes the port.

VPN is usually safer and more reliable than public port forwarding.

### Public IP or DDNS with port forwarding

Use this only when VPN is not available and the collector must reach Home Assistant from outside your network.

Example:

- Home Assistant local IP: `192.168.1.50`
- Router forwards public `203.0.113.10:50099` to `192.168.1.50`
- Remote collector IP: `198.51.100.44`

Then use:

- **Local listener IP**: `192.168.1.50`
- **Collector IP**: `198.51.100.44`
- **Discovery target**: `198.51.100.44`
- **Advertised callback IP**: `203.0.113.10`
- **Advertised callback TCP port**: `50099`

Your router/firewall must allow the collector to connect back to Home Assistant.

## Step by step

1. Open **Settings → Devices & Services → EyeBond Local**.
2. Try normal scan first if the collector is local.
3. If the collector is remote, choose **Manual setup**.
4. Enter **Local listener IP**.
5. Enter **Collector IP** if you know it.
6. Open **Advanced connection settings**.
7. Fill **Advertised callback IP** and **Advertised callback TCP port** only for VPN/NAT/port-forwarding cases.
8. For remote setup, set **Discovery target** to the exact collector IP.
9. Save and wait. Some collectors reconnect slowly.
10. If setup stays partial, create a Support Archive.

## Troubleshooting

### The collector is not found

Check:

- the collector IP is correct;
- Home Assistant can reach that network;
- VPN is connected, if you use VPN;
- the router/firewall is not blocking the probe.

### The collector is found, but never connects back

Check:

- **Advertised callback IP** is reachable from the collector side;
- **Advertised callback TCP port** matches your forwarding/VPN setup;
- your router forwards the connection to the Home Assistant host;
- Home Assistant is listening on the selected local IP.

### Local setup stopped working after changing advanced fields

Return to the safe local configuration:

- clear **Advertised callback IP**;
- clear **Advertised callback TCP port**;
- restore **Discovery target** to normal local discovery;
- make sure **Local listener IP** is the Home Assistant LAN IP.

## Safe default

If you are not sure whether you need remote setup, you probably do not.

Use the normal setup wizard and keep the collector on the same network as Home Assistant.
