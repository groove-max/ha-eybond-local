# Setup and Discovery

This guide explains how EyeBond Local finds and adds a collector. The normal
setup is intentionally collector-first: Home Assistant confirms the collector,
creates the integration entry, and then identifies the inverter while the
integration is running.

## Before you start

- Put the collector on Wi-Fi or Ethernet where Home Assistant can reach it.
- Keep the collector powered and connected to the inverter.
- If possible, use a Home Assistant network interface on the same subnet.
- Do not remove an existing working entry just to run another scan.

If the collector is at another site or behind NAT, read the
[Remote / NAT Setup Guide](REMOTE_SETUP.md) before entering advanced addresses.

### If the collector is not on your network yet

The first setup screen offers two normal paths:

- **Bluetooth Wi-Fi setup** — choose the collector, select or enter the Wi-Fi
  network, and send its credentials. Home Assistant then returns to the normal
  network scan. This needs a local Bluetooth adapter or an ESPHome Bluetooth
  Proxy near the collector.
- **Collector access point** — connect the Home Assistant host to the Wi-Fi
  network broadcast by the collector, then continue with the normal scan. The
  network name usually contains the collector identifier and some collectors
  use `12345678` as the default password.

You can also use the vendor app to put the collector on Wi-Fi first. Bluetooth
is used only for Wi-Fi provisioning; normal monitoring remains local network
traffic.

## Normal setup

1. Open **Settings → Devices & Services → Add Integration**.
2. Choose **EyeBond Local**.
3. Confirm that the collector is already on your network.
4. Choose the Home Assistant network interface and start the scan.
5. Select the device you want to add.
6. Confirm the collector when Home Assistant asks.
7. Choose the sensor refresh mode and finish setup.

The collector device is created first. The inverter device can appear a little
later, after runtime detection completes. This is expected and does not mean
that setup created a collector-only installation.

## What one scan checks

One scan combines three sources:

- collectors already connected to EyeBond Local;
- replies to the local EyeBond discovery request;
- a bounded `/24` address check when broadcast discovery is not enough.

The progress indicator describes the whole scan, not one request per visible
address. Some stages can take longer than others, so progress may advance in
larger steps. It must not move backwards.

EyeBond Local does not enumerate every address in a large `/16`. Use the correct
subnet broadcast, scan from the matching Home Assistant interface, or enter a
known collector address manually.

## Understanding scan results

| Result | Meaning | What to do |
|---|---|---|
| **Ready to set up** | The collector identity and a usable route are known. | Select it and continue. |
| **Needs confirmation** | The collector identity is known from an incoming connection, but the peer address may be a router or NAT address. | Select or enter an address that Home Assistant can use to reach this collector. |
| **Check address** | An address answered discovery, but the collector identity is not yet proven. | Select it to run the identity check. |
| **Already configured** | The collector identity already belongs to an EyeBond Local entry. | Open the existing entry instead of adding a duplicate. |

An incoming TCP address is not automatically treated as the collector's own
address. Routers, port forwarding, and NAT loopback can make several collectors
appear to connect from the same IP. Home Assistant therefore asks for an address
only when it cannot safely bind the observed identity to one reachable route.

## What the verification step does

After you select a collector, Home Assistant verifies that the selected route
still belongs to the same collector identity. Depending on the collector, this
can include a reconnect or restart and a callback request.

The entry is created only from a verified collector identity. A timeout,
identity mismatch, or ambiguous set of new sessions stays inside the setup flow
and offers a retry; it does not create a placeholder or Pending device.

If a retry is offered, it retries the selected collector attempt. Use
**Scan again** only when you want a new network scan.

## Background discovery

Use **Enable background discovery** when a collector is not connected yet but
is expected to connect to Home Assistant later.

Background discovery creates the persistent **EyeBond Local — Discovery** entry.
It does not create a collector or inverter device. When an unconfigured
collector later provides a reliable identifier, Home Assistant publishes a normal
discovery card and the same collector-first admission flow continues from there.

The normal scan also includes identified sessions that are already connected.
A collector found through background discovery is therefore not hidden from
the regular **Add Integration** workflow.

## Manual setup

Use **Device not found? Advanced setup → Enter address manually** when:

- you know the collector address;
- broadcast discovery cannot cross the network;
- the collector is remote or behind NAT;
- a developer asks you to use a specific identity probe.

Keep the default automatic driver and fast detection mode unless you have a
specific reason to change them. The choices for querying a silent collector
with the framed or AT command protocol are diagnostic recovery paths, not two
collector models to guess between. Use them only when the normal attempt or a
developer points to that wire format.

Manual setup may ask how the collector should connect:

- **Collector connects to Home Assistant on its own** — use this only when the
  collector already points at the Home Assistant listener.
- **Ask the collector to connect when needed** — Home Assistant sends a request
  to the collector address when it needs a callback connection.

These are setup routes. The normal product choice after setup is shown as
**Cloud + Home Assistant** or **Home Assistant only** under
**Collector connection and cloud**.

## Common problems

| Problem | What to try |
|---|---|
| Nothing is found | Retry with the correct Home Assistant interface. Check collector power, Wi-Fi, and subnet. |
| Only an address is shown | Select it and let the identity check run. Do not assume the reply IP is the final collector identity. |
| A connected collector needs an address | Choose the route Home Assistant can actually reach; the incoming peer may be a router or NAT address. |
| The collector connected but did not identify itself | Retry once with the collector stable. If it repeats, create a Support Archive. |
| The collector did not reconnect in time | Keep the flow open and retry. Check callback routing and Wi-Fi before scanning again. |
| `Invalid flow specified` appears after the device was created | Refresh **Devices & Services** first. If the entry exists, do not add it again; create a Support Archive because the flow should close cleanly. |
| Only the collector device appears afterward | Continue with [Runtime Detection and Entities](RUNTIME_AND_INVERTER.md). |

## Next step

After the entry is created, see [Runtime Detection and Entities](RUNTIME_AND_INVERTER.md)
for driver selection, Fast versus Full detection, polling, controls, and entity
availability.
