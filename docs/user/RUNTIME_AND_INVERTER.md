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

## Control mode

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

Classic SMG-family inverters use a separate documented RS232 V1 register map.
The document version is not treated as an inverter identity: compatible devices
have been observed with layout values 1, 2, and 11. EyeBond Local therefore
shares only the documented controls, then applies validation and any additional
registers in the exact model profile. On the maintainer-tested SMG 6200,
hardware-confirmed controls remain available in Auto mode. An unknown SMG
fingerprint remains read-only instead of inheriting writes from a similar model.

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
