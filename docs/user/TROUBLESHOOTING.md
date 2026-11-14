# Troubleshoot EyeBond Local connections and readings

[Українська](TROUBLESHOOTING.uk.md)

## HA address after a restart or network change

Current main preserves the configured HA listener/callback address even if its
LAN interface is temporarily absent during startup. Automatic selection is only
used when no address is configured, and excludes known Docker/internal host
interfaces. The TCP listener's wildcard bind address is not an address to give
to the collector.

If an older build replaced the LAN address with `172.30.32.1` (`hassio`) or a
Docker bridge address, restore the correct address through the existing entry's
connection settings. Updating does not guess the former address. If HA's LAN IP
really changed, update it explicitly there. Preserve any separately configured
advertised/NAT address and port; do not delete the entry or change the inverter
profile, UART or vendor-cloud endpoint for this issue.

Keep the existing setup and collect evidence before resetting or deleting the
integration. A collector connection, inverter response and vendor-cloud update
are different stages; identify which one stopped.

## Collector not found or callback fails

Check collector Wi-Fi, HA's selected interface and the return route from the
collector. For another subnet, enter its known IP instead of expecting local
broadcast to cross the router. A reply on UDP 58899 is not proof of a TCP
callback or verified identity. [Connection guide](GETTING_STARTED.md) and
[advanced routed setup](REMOTE_SETUP.md) explain the addresses and ports.

## Collector exists but inverter sensors do not

Runtime identifies the inverter after the collector is added. Check **Poll
Context** and detection status; verify the inverter cable and UART settings
against the manufacturer's information. Do not cycle through unrelated drivers.
Create a [Support Archive](SUPPORT_ARCHIVE.md) if detection cannot complete.

## Readings are unavailable, stale or zero

An unavailable sensor is not the same as a disabled entity. Check the entity's
enabled state and whether the whole device is offline. Compare live readings
with the inverter display, noting time and time zone. Some energy/flow sensors
are estimates: a zero without a supported direct measurement is not independent
proof of no physical power flow. Report the exact sensor and a same-time reading.

For intermittent gaps, retain automatic polling or increase an overly short
manual interval. **Poll Duration**, **Poll Utilization** and **Recommended Poll
Interval** help explain whether requests fit. Faster polling is not a general fix.

Development builds after 0.3.0-rc.1 expire cached inverter measurements after
a complete read outage of three poll intervals, with a minimum of 60 seconds.
Expiry is applied when the runtime next builds a snapshot; a collector heartbeat
alone does not renew it. The collector may therefore remain connected while its
inverter readings become unavailable. Short misses and healthy partial updates
are tolerated; after a long gap, only newly received fields return. Estimated
energy stops accumulating when its source power expires. This does not itself
repair the lost inverter stream. Save a Support Archive and the existing log
before requesting a callback if practical; do not deliberately prolong an outage.

<a id="cloud-updates-stop"></a>
## Cloud updates stop while HA works

First distinguish intentional disconnection (**Home Assistant only**, proxy
transition or a learning protection hold) from **Cloud + Home Assistant**.
Some users see cloud telemetry pause in the latter mode while HA readings keep
changing. Its cause and a general fix remain unconfirmed; neither a restart nor
a longer interval is a proven universal solution.

When practical, collect a Support Archive during the event before restarting.
Report the version/commit, mode, local time/time zone, last cloud data time,
whether HA still updates and when the cloud recovers. A cloud timestamp can
use another time zone or represent history rather than a live sample.
Continue your existing issue or open a new support issue; do not reset the
collector merely to prepare a report.

## Missing settings or changes that revert

Start with **Auto**. **Full Control** only exposes available controls; it cannot
create a missing map or guarantee a write. For a cloud setting that is absent
locally, see [Write Controls learning](LEARNING.md). A rejected or unconfirmed
write needs read-back and diagnostics, not arbitrary wider limits or repeated
commands. Avoid simultaneous changes from HA and the vendor app.

## After replacing files

Fully restart HA. Keep only the intended integration directory inside
`custom_components`; backup copies with the same manifest can load old code.
Record the commit for main builds, even if the displayed version is beta.5.
[Installation and rollback](INSTALLATION.md).

## Still stuck?

[Prepare a useful support report](HELP.md). Do not post passwords or provide
remote access publicly. Support may request a bounded debug log or capture;
you do not need to run every diagnostic tool in advance.
