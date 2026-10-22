# Troubleshoot EyeBond Local connections and readings

[Українська](TROUBLESHOOTING.uk.md)

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
