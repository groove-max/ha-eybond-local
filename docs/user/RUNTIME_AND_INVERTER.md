# Runtime Detection and Entities

EyeBond Local adds the collector first and identifies the inverter while the
integration is running. This guide explains what happens after setup and how to
use **Polling and inverter detection**.

## What happens after setup

1. Home Assistant owns one verified collector session.
2. EyeBond Local probes supported inverter protocols through that exact session.
3. A driver is accepted only after a reliable protocol response.
4. The model catalog uses local fingerprints or identity fields to choose the
   closest safe model profile.
5. Home Assistant creates or updates the inverter device and its entities.

The inverter may appear after the collector. A slow protocol or a Full scan can
take more than one normal polling interval. **Poll Context** shows whether the
integration is detecting an inverter, reading it, or only checking the collector.

## Polling and inverter detection

Open **Settings → Devices & Services → EyeBond Local → Configure → Polling and
inverter detection**.

### Inverter driver

- **Auto** is recommended. EyeBond Local tests compatible drivers and keeps the
  first or complete set of confirmed results according to the detection mode.
- Choose a specific driver only when the model is already known or a developer
  asks you to do so.

Changing the driver starts a new identification. It does not change the
collector endpoint or cloud profile.

### Automatic identification mode

- **Fast: first confirmed protocol** is the normal default. It stops after the
  first reliable driver match and gives the shortest setup time.
- **Full scan: check all protocols** tests every supported driver. Use it when
  an inverter is known to answer through more than one protocol, or when a
  developer asks for a complete comparison.

Full scan can take noticeably longer. It is not a deeper network scan and does
not search more collector IP addresses; it only checks more inverter protocols
through the already connected collector.

### More than one protocol matched

If a Full scan confirms multiple protocols, the options menu shows
**Choose inverter protocol**. Select the protocol that matches the expected
model and readings. The selection deliberately changes Control mode to
**Read-only** while the runtime confirms the chosen driver. Review the new
readings first, then change Control mode back to **Auto** if they are correct.

You can return to **Auto** later to run detection again.

## Sensor refresh mode

- **Automatic** lets EyeBond Local choose a safe start-to-start interval from
  real device response time and protocol limits.
- **Manual** uses the interval you enter. It does not make a slow device answer
  faster.

Useful diagnostics:

- **Poll Duration** — how long the latest cycle took.
- **Poll Utilization** — how much of the current interval is spent polling.
- **Recommended Poll Interval** — a safer interval based on observed timing.
- **Poll Context** — whether the runtime is reading, detecting, or recovering.

If utilization remains high, use Automatic mode or increase the manual
interval. Occasional long cycles during detection or reconnect recovery are not
the same as continuously overloaded polling.

## EyeBond Short-ASCII family

This read-only profile is included in the unreleased test code. It supports
one confirmed short-command protocol seen on some Anern and Maxinn inverters;
the brand name alone does not establish compatibility.

After normal collector setup, keep inverter detection on **Auto**. A matching
device appears as **EyeBond Short-ASCII family** because the available replies
do not reliably identify its commercial model or serial number.

Available readings are grid voltage, output voltage, load percentage, output
frequency and inverter temperature. Diagnostics also include firmware, fault
and connection flags, and **Battery Reference Voltage**. That last reading is
the protocol's single-block reference, not the voltage of the complete battery
pack; do not use it as a replacement for a 24/48 V pack measurement.

Some compatible devices also answer optional requests for **BMS Battery Voltage**,
**Battery State of Charge**, BMS temperatures, cell voltages, cycle count,
protection limits, charge/discharge path flags and rated values. When the
device's RH settings report BMS current display accuracy **with decimals**,
**BMS Charging Current**, **BMS Discharging Current** and **Battery DC Power**
may also appear (scaled per the vendor decimals path). If RH reports without
decimals, or RH has not been read successfully yet, those current/power
entities stay unavailable rather than guessing a scale. These entities are
disabled by default: open the inverter's entity list and enable the ones you
need. A charge-path flag means the path is enabled, not that the battery is
currently charging. BMS voltage is separate from Battery Reference Voltage;
neither reading is calculated from the other. Battery DC Power is measured from
BMS voltage and currents (positive while charging); it is not the labelled
**Estimated AC Load Power** from load percentage × rated VA.

BMS is requested no more often than every 30 seconds, rated values and RH
settings every 15 minutes, with at most one extra request per normal poll. A
longer poll interval can delay them further. Failed or invalid responses
immediately remove the old values for that group. At each refresh, BMS samples
aged 60 seconds or more and rated/RH values aged 15 minutes or more are
discarded. If the device returns the known
no-data BMS reply, **BMS Data Available** turns off and its measurements become
unavailable, even if some fields still contain old numbers. This does not
prove the physical battery is disconnected.

After four failed optional requests while basic telemetry still responds,
that request is skipped. Use **Re-check supported commands** to try it again,
for example after connecting a BMS. Missing optional data does not prevent
basic inverter monitoring.

This profile does not provide PV power, grid frequency or inverter controls.
Selecting **Full Control** does not add undocumented settings. If readings are
missing or implausible, create a Support Archive for review; it can include the
optional raw replies. Do not select a similar retail model by guesswork.

## Control mode

The optional **Write Capabilities** and **Blocked Write Capabilities** diagnostic
sensors show how many settings are listed. Open the entity's attributes to see
the complete list under `capabilities`. This is a diagnostic inventory, not a
promise that every listed setting is enabled in your current Control mode.
If another diagnostic text is too long for a Home Assistant state, its full
text is available in the `full_value` attribute and in the Support Archive.

Control mode is independent from the collector's cloud connection profile.

- **Read-only** hides inverter writes and keeps monitoring.
- **Auto** exposes controls confirmed for the detected model and is recommended.
- **Full Control** exposes every available driver control, including advanced
  items. It does not turn an unverified control into a tested one. Operations
  explicitly marked blocked, such as an unvalidated factory reset or counter
  erase, remain unavailable even in Full Control.

The inverter itself is the final authority for a write. EyeBond Local sends the
requested value and checks readback when the protocol supports confirmation. A
rejected or unchanged value is reported instead of being treated as success.

Some experimental models have a large document-backed settings surface whose
writes have not yet been confirmed on that exact hardware. Those entities stay
hidden in Auto mode and disabled by default even after Full Control is selected.
Enable only the individual settings you intend to test. Kevolt 8 kW users
should read [Kevolt / Deye-Compatible Advanced Controls](KEVOLT_DEYE_CONTROLS.md)
before enabling them.

For inverters using the documented Anenji Communication Protocol No. 3-10,
the integration selects a version-specific write matrix from the inverter's
reported protocol number. Protocol 3/5 never inherit Protocol 4/6-only OP2
settings, and fields without a valid protocol number remain model-specific.
These document-backed controls are untested until confirmed on an exact model,
so Auto mode does not expose them.

The reported protocol number also selects a version-specific telemetry and
control map for an inverter model that is not yet listed in the catalog. Normal
monitoring starts without pretending that a similar commercial model was
detected. All document-backed controls remain marked untested and are available
only after the user explicitly selects Full Control; Auto mode stays
monitoring-only. Protocol 3/5 and Protocol 4/6 use their documented
output-register locations, and fields documented only for Protocol 3/4 are not
projected on Protocol 5/6. An exact catalog model still takes priority when
available.

The same approach now covers unknown SMG models reporting protocol **1, 2, or
11**. They appear as **SMG Protocol N (Unverified Variant)**, not as a guessed
brand or model. Protocols 1 and 11 have a compatible basic settings set;
protocol 2 has its own documented GM6200 settings, including schedules and the
inverter clock. Protocol 11 support is based on the common documented map and
device captures, not a complete vendor specification for that number.

To try these settings, select **Full Control** under the inverter's control
settings. Some controls become visible immediately; advanced settings remain
disabled until you enable the individual entity. All generic-profile controls
are marked **untested**. Selecting Full Control does not itself send any
settings to the inverter. Operations explicitly marked blocked remain
unavailable. Share a support archive and the settings you actually verified if
you want to help confirm support for your model.

Exact model profiles still take priority and keep their existing tested
controls. In particular, the maintainer-tested SMG 6200 keeps its normal Auto
mode controls. A larger protocol number does not mean that it supports all
registers from smaller numbers; unsupported numbers are not assigned a guessed
map. If a setting is missing, [active device learning](DEVICE_LEARNING.md) can
sometimes identify additional cloud controls for the particular inverter.

## Available, unavailable, and disabled entities

The entity registry can contain more entities than your device page shows.

- **Available** — the current driver supplied a valid value.
- **Unavailable** — the current collector, firmware, or inverter did not supply
  that optional value. A few diagnostic entities can legitimately stay
  unavailable.
- **Disabled by the integration** — an advanced, model-inapplicable, duplicate,
  or diagnostic entity is kept in the registry but is not enabled by default.

A large disabled count is not by itself a fault. Check the normal PV, battery,
load, grid, status, and control entities first. Do not enable every disabled
entity at once; many are intended only for another model variant or advanced
diagnostics.

Typed telemetry keeps the source and freshness of each runtime value. A failed
supplemental metadata read cannot overwrite a current measurement or SSID with
an empty value.

## When identification does not finish

1. Confirm that the collector remains connected.
2. Check **Poll Context** and **Runtime Driver State**.
3. Leave the driver on Auto and use Fast mode for one clean retry.
4. If the inverter is known to support several protocols, try Full scan once.
5. If no driver binds, create a [Support Archive](SUPPORT_ARCHIVE.md).

Do not repeatedly remove and re-add the collector to restart inverter
detection. Change the driver or detection mode, or reload the entry after
collecting a Support Archive.

## Related guides

- [Setup and Discovery](SETUP_AND_DISCOVERY.md)
- [Collector Management](COLLECTOR_MANAGEMENT.md)
- [Device Learning](DEVICE_LEARNING.md)
- [Inverter Model Catalog](../generated/INVERTER_MODEL_CATALOG.generated.md)
