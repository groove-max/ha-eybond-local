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

### PI30 names shared by several brands

Some inverters report a firmware identifier instead of their retail model.
For example, `VMII-NXPW5KW` appears on both PowMr and Victor units, including
different power ratings. These devices appear as **PI30 VMII-NXPW5KW**; the
name alone does not mean 5 kW, 4.2 kW, or a particular brand. Rated power is read
separately from the inverter.

Older builds labelled this shared profile **PowMr 4.2kW**. The corrected name
keeps the same protocol, profile, controls and entity IDs. You do not need to
delete and add the device again. A custom name set in Home Assistant is retained.
The model catalog lists confirmed retail models separately; it does not turn a
shared firmware identifier into a unique brand match.

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

## MUST PV/PH18 power and energy corrections

The unreleased test build corrects several readings in the MUST PV/PH18 map:

- **Load Power** uses the load measurement, not the inverter converter's power.
  The existing entity ID is retained. **Inverter Power** is a separate signed
  measurement; a negative value is not a 65 kW load. **AC Output Power** remains
  the same underlying load measurement for existing dashboards.
- **PV Energy Total** is the cumulative hardware counter in kWh. It replaces
  the incorrectly scaled **PV Generation Sum** with a new entity and fresh
  statistics. If you used the old counter in Energy Dashboard or an automation,
  select the new sensor after updating.
- **PV Generation Day** was actually days of operation, not daily generation.
  That energy entity is retired. The optional **PV Charger Operating Days**
  diagnostic exposes the value with the correct unit and is disabled by default.

The integration does not rewrite historical statistics or automatically change
your Energy Dashboard configuration. Earlier totals and energy estimates based
on the incorrect load reading may need review. These corrections do not change
the collector connection, polling ranges or inverter controls.

**Estimated Load Energy Today** is calculated by Home Assistant from **Load
Power** over time; it is not a daily register read from the inverter. If the old
version reported an incorrect high load, that energy may already be included in
today's total. Updating or restarting HA on the same day retains that total.
It resets on the first valid reading of the next day in HA's configured timezone.
If the collector is offline at midnight, the displayed total can remain until
readings resume. The reset does not remove incorrect historical statistics.

After updating, compare Load Power with the manufacturer's app while a load is
present, then check the next day's estimate. If it still grows unexpectedly,
include the Load Power and Estimated Load Energy Today history around that time
with a fresh Support Archive. Do not change scaling or delete the integration
just to clear the old total.

### PV3300 direction and load percentage

For a device identified as **MUST PV3300**, the test build also corrects **Load
Percent** and the signs used for battery/grid energy flows. Positive **Battery
Power** and **Battery Current** mean charging; negative means discharging.
Positive **Grid Power** means import; negative means export. The manufacturer's
app may use the opposite convention. Native **Inverter Power** is a separate
measurement and keeps its original sign.

Already added, confirmed PV3300 devices receive the corrected map after updating
and restarting HA; do not remove and re-add them. Existing entity IDs and
history are kept. Earlier battery charge/discharge estimates are not repaired
retroactively. Other MUST models keep their existing interpretation.

Custom or learned maps are not rewritten. A learned map tied to the older
generic MUST schema may stop applying after the base map changes; it needs to
be regenerated for PV3300 rather than having its compatibility check bypassed.

MUST AC current readings are named **Inverter Current**, **Grid Current**, and
**Load Current**. The first two were previously called **Output Current** and
**AC Output Current**; entity IDs and history are kept. Grid Current can be zero
when the grid is disconnected. If the other currents stay at zero despite a
load, create a Support Archive: for PV3300 it compares the combined response
with individual reads. The integration does not replace a reported zero with
an estimated current.

### MUST controls and battery percentage

The MUST profile contains 27 document-backed controls. For a locally identified
**PV3300**, four are marked tested: **Grid Max Charge Current**, **Max Combined
Charge Current**, **Charge Source Priority**, and **Energy Use Mode**. They are
available in **Auto** with high-confidence detection. The
[issue #46 owner confirmation](https://github.com/groove-max/ha-eybond-local/issues/46#issuecomment-5939601570)
reports successful local changes and HA readback, not independent validation of
physical behavior or every supported value. The other 23 PV3300 controls and
all controls on other MUST variants remain **untested** locally.

To opt in to untested controls, open **Configure → Polling and inverter detection → Control mode → Full Control**.
The controls belong to the inverter device's configuration section, not the
collector. Selecting Full Control does not send a command or change inverter
settings. **Off-Grid Output** is specifically the off-grid output enable; it is
not a general inverter power switch or the separate cloud **Ongrid Switch**.

**Upgrade note:** older builds incorrectly treated 20 cloud-listed controls as
tested. Only the four PV3300 controls above now qualify for **Auto**. Use Full
Control if you choose to test the others; review automations that referenced the
earlier controls. Updating and restarting HA refreshes confirmed PV3300 bindings
without re-adding the device. **Read Only** still blocks all controls. Telemetry
and the selected control mode are not changed. No local hardware qualification
is implied by the existing broad setpoint limits; use only settings appropriate
to your exact model and battery.

PV3300 devices with supported BMS communication now expose **Battery State of
Charge** directly from the BMS, not an estimate from battery voltage. Separate
**BMS Battery Voltage**, **BMS Battery Current** and **BMS Battery Temperature**
appear under inverter diagnostics. Update the main test build and restart HA;
there is no need to re-add the device or enable Full Control for these readings.

These BMS readings do not replace the inverter's existing battery measurements.
**BMS Battery Current** keeps the manufacturer's signed value; do not assume it
uses the same direction convention as **Battery Current**. Battery capacity in
Ah is a setting, not remaining charge.

The optional BMS request has a three-second limit. Missing or invalid readings
become unavailable, not zero or a frozen last value. Other inverter telemetry
continues; after a failure or an empty BMS response the integration retries after
one minute, or five minutes if the device explicitly rejects the command/address.
A genuine 0% SOC with valid BMS data remains 0%. Other MUST models keep their
existing map. A Support Archive also includes a separate bounded raw BMS read
for troubleshooting.

## SRNE partial battery / PV readings

Some SRNE firmware rejects a combined battery/PV request while answering smaller
documented groups. The test build tries those groups only after an explicit
unsupported-address reply, not after a timeout. Available readings continue
updating; unsupported groups stay unavailable. No setup change is needed, and
this does not add inverter controls.

## Hopewind / Bluesun profile

The test build adds **Hopewind String (Protocol 0237)** telemetry, checked against
local register readings from a Bluesun BSM15K-B. Keep detection on **Auto**.
The integration shows a protocol-family name because these replies do not prove
an exact retail model.

Readings include PV voltage and power, AC generation, line-to-line grid
voltages, phase currents, frequency, temperature, and daily/total PV energy.
**Inverter AC Active Power** is generation from this inverter, not household
consumption or net import/export at your electricity meter. No battery or home
load measurements are inferred.

Additional MPPT/string channels and raw fault diagnostics are disabled by
default. Only enable channels actually present on your inverter; the family
map includes more channels than some models have. Cloud analysis remains a
separate feature and is not required for local telemetry.

**Full Control** additionally exposes three **untested** settings: active-power
regulation mode, active-power ratio (0–100%), and reactive-power regulation mode.
The register addresses and current values were confirmed on a BSM15K-B, but
local writes still need owner validation. Auto and Read-only do not expose these
controls. Updating or selecting Full Control does not change any settings.

Changing a regulation mode uses the inverter's existing setpoint; it does not
set a new limit automatically. The existing kW, power-factor and reactive-ratio
setpoints are available as diagnostics. Only use values appropriate to your
installation. These are inverter-generation controls, not a site-wide
zero-export configuration.

The documented active-ratio write range stops at 100%. A firmware value such as
110% is displayed as received, not silently changed or used to widen that range.
Absolute-power and signed reactive/power-factor writes are not included yet;
they need rated-power-dependent limits or additional numeric validation.

Creating a Support Archive reads these settings too. It does not change those
settings. SmartClient currently supports
read-only cloud analysis, not active control learning; selecting Full Control
does not bypass that limitation.

## EyeBond 09C1 family

The unreleased test code includes this read-only profile for the protocol
captured on a ZL Power GSIII. Keep detection on **Auto**. A matching inverter
appears as **EyeBond 09C1 family**: the replies identify the protocol, not a
unique retail model or serial number. A similar brand/model name alone is not
enough to establish compatibility.

Readings include grid and output voltage/frequency, battery voltage, load
percentage, temperature, PV voltage/current and operating status. Grid frequency
and output frequency are read separately; neither substitutes for the other.
Rated values are diagnostics, disabled by default. **Rated Power** is not a
measurement of actual output power. **AC Charger Enabled** describes the charger
state, not a measured charging current or power.

**Grid Fault Voltage** is a separate field named that way in the manufacturer's
protocol, kept under diagnostics and disabled by default. Enabling it makes its
reading available for dashboards too; the diagnostic category does not change
the value. It is not automatically substituted for Grid Voltage based on one
comparison with a meter.

The legacy **PV Voltage/Current** readings still come from `PV?`, not a sum of
two inputs. Optional `PV1` and `PV2` reads provide separate voltage/current
channels after both commands have returned valid samples in the current runtime.
Their numbers follow the **protocol commands**, not guaranteed physical socket
labels: the issue #50 owner's unit has crossed labels. No global port swap,
combined current/power, or guessed energy total is applied. The owner's raw
captures were taken at different times, not as a simultaneous total.

Allow at least two polling cycles for the separate readings to become available.
The channels are checked in turn, less frequently than the main readings, to
limit extra load on the collector. Missing or outdated values become unavailable;
they are not replaced with the other channel's value. After repeated failures,
use **Re-check supported commands** to retry. The old `PV?` readings continue
independently. A Support Archive includes both responses and sample ages.

For mains presence, use **Grid Available**. An absent grid during normal battery
operation is not an inverter fault. A dashboard expecting cloud `Status` enum
names needs an appropriate input/mapping; the cloud's `FAULT` label is not a
local mains-presence signal and is not synthesized here.

If a PV, output-frequency or rated-values request fails, that group's old values
become unavailable while basic readings can continue. If the main status request
fails, normal connection recovery applies. No inverter serial, battery percentage,
measured power or energy counters are guessed. **Full Control** does not add
controls: a verified command map is not available for this profile yet.

This is separate from the Short-ASCII profile below. Local inverter support also
does not repair a collector's saved cloud-server address; those settings remain
under **Collector connection and cloud**.

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
protection limits, charge/discharge path flags and rated values. These entities
are disabled by default: open the inverter's entity list and enable the ones
you need. A charge-path flag means the path is enabled, not that the battery is
currently charging. BMS voltage is separate from Battery Reference Voltage;
neither reading is calculated from the other.

For example, a Battery Reference Voltage near 12.5 V is not a claim that your
48 V battery bank has dropped to 12.5 V. If the inverter does not answer the BMS
request, full-pack voltage remains unavailable; we do not multiply the reference
by a guessed cell/block count. Temperature is supplied in °C; Home Assistant
can display °F according to its unit settings or the entity's unit override.

BMS is requested no more often than every 30 seconds, rated values every
15 minutes, with at most one extra request per normal poll. A longer poll
interval can delay them further. Failed or invalid responses immediately
remove the old values for that group. At each refresh, BMS samples aged
60 seconds or more and rated values aged 15 minutes or more are discarded.
If the device returns the known
no-data BMS reply, **BMS Data Available** turns off and its measurements become
unavailable, even if some fields still contain old numbers. This does not
prove the physical battery is disconnected.

After four failed optional requests while basic telemetry still responds,
that request is skipped. Use **Re-check supported commands** to try it again,
for example after connecting a BMS. Missing optional data does not prevent
basic inverter monitoring.

This profile does not provide PV power, BMS currents, battery power, grid
frequency or inverter controls. Selecting **Full Control** does not add
undocumented settings. If readings are missing or implausible, create a
Support Archive for review; it can include the optional raw replies. Do not
select a similar retail model by guesswork.

## Control mode

For **Anenji ANJ-6200-48PL** (layout 2/model `0x2300`), Output Source Priority
uses **SUB / SBU / SUF / ZEC**, not the SMG 6200 mode names. The owner has confirmed
all four selections in HA and on the inverter display, so this selector is
available in **Auto** as well as Full Control; Read-only still blocks it.
This confirms switching the setting, not the electrical operation of grid export
or the CT installation. SUF permits grid export; ZEC requires the external CT
configuration described in the inverter manual. A corrected label
does not remove inverter-side restrictions: rejected writes are still reported.
Updating does not change the selected inverter mode. Existing entries do not
need to be removed and added again.

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
