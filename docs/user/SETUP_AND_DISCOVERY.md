# Setup and Discovery

This guide explains how EyeBond Local finds and adds a collector. The normal
setup is intentionally collector-first: Home Assistant confirms the collector,
creates the integration entry, and then identifies the inverter while the
integration is running.

## Before you start

### MarsRock grid-tie: limited manual read profile

The **MarsRock grid-tie (manual; read-only)** driver (`marsrock_grid_tie`)
is available for the captured SUN-style register map, not every MarsRock or
SUN-branded inverter. Select it explicitly in the driver's settings; automatic
detection deliberately does not select it. Keep your existing collector entry.

It exposes grid voltage, DC input voltage, protocol-reported generation power
and total generated energy. Generation is not household load; DC input is not
assumed to be solar (the tested capture comes from a wind installation).
The power value is not yet established as an instantaneous LCD reading.
Temperature, frequency, status flags and controls are not qualified and are
not exposed just because their registers return zeros. Full Control does not
add controls to this read-only map; active learning is not enabled for it.

The decoder has been tested against captured cloud RTU replies. This does not
prove the collector's local callback tunnel works: the existing transport
adapter uses the standard Modbus tunnel, or raw Modbus on an AT session.
If explicit selection still leaves the inverter unidentified, collect a support
archive rather than changing endpoint, UART settings or trying other write maps.
Field testing is still needed before calling this profile hardware-confirmed.

For an identified inverter, creating a support archive also performs a bounded
read-only comparison of five candidate registers (60, 94, 99, 107 and 110) and
small samples of documented energy/fault history. It takes up to 25 seconds,
with one-second pauses between planned reads. Regular polling waits for this
capture; no settings or collector endpoint are changed. An illegal-address reply
is recorded and skipped; a timeout or other communication error stops the plan.
The archive keeps zero, missing and failed reads distinct. These observations
do not enable sensors or controls automatically. When sharing the archive,
include the approximate capture time, LCD temperature/power and whether the
inverter was generating. There is no need to force generation or change settings.

### Network prerequisites

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

### Include known addresses on another network

Choose **Device not found? Advanced setup → Add known collector IPs to scan**.
Enter up to eight collector IPv4 addresses separated by commas, then submit.
The normal local scan remains enabled; these addresses are additional direct
targets. **Scan again** keeps the list within this setup flow. Open the same
form and clear it to return to local-only discovery.

Use this when routing/VPN already lets the collector reach the selected
Home Assistant listener. It does not discover routes through routers, scan an
entire remote subnet, or forward local broadcast packets. If the collector
needs a different callback address or port, use **Enter address manually**
instead; see [Remote / NAT setup](REMOTE_SETUP.md).

The list is not a background scanner or a second runtime configuration. It is
discarded when this setup flow ends. After adding a collector, its verified
connection settings belong to that collector's entry.

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

## Repairing an older unidentified entry

An older entry may have worked without storing the collector's identity. After
updating, TCP connections alone cannot bind it safely to the integration. The
options dialog explains when this repair is needed. Full scan, Full Control,
or forcing an inverter driver cannot replace collector identity confirmation.

If an older installation still shows **EyeBond Setup Pending** or reports that
the collector identity needs verification, keep the entry. Open its three-dot
menu under **Devices & Services → EyeBond Local** and choose **Reconfigure**.
Check the collector address and connection settings, then submit the form.
On a server with multiple network interfaces, keep the Home Assistant interface
that can be reached from the collector's network. The form preserves the saved
interface when it is still available. If that interface no longer exists, select
its replacement; for NAT, also check the advertised callback address and port
in the advanced settings.

If the collector connects without identifying itself, the result offers
**Silent collector: query with EyeBond framed protocol** and **Silent collector:
query with AT command protocol**. Use the protocol recommended for your case.
This sends one read-only identity query, without restarting the collector or
changing inverter settings. It does not automatically try another protocol.
If that connection has closed, choose **Probe again** to obtain a new result.

A successful check repairs the existing entry and reloads it; it does not add
a duplicate or change the cloud/control mode. Failure leaves the entry intact.
Inverter detection starts afterward and is a separate check.

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
