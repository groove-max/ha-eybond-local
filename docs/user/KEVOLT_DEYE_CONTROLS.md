# Kevolt / Deye-Compatible Advanced Controls

EyeBond Local can identify the reported **Kevolt PD0080G-TPM-EU 80 kW**
inverter by its exact local Modbus fingerprint. Normal monitoring works without
enabling any advanced control.

The available protocol document also describes many inverter settings. EyeBond
Local exposes 118 of the settings whose address, encoding, scale, and safe
read-back method are unambiguous. They include:

- battery and charging limits;
- generator and smart-load settings;
- export and meter settings;
- six Time of Use periods, including their time fields;
- grid reconnection, peak-shaving, topology, and selected advanced options.

These controls are **document-backed but not yet write-tested on this exact
hardware**. They are therefore hidden in **Auto** mode and disabled in the Home
Assistant entity registry by default.

## Enabling one control for testing

1. Open **Settings → Devices & Services → EyeBond Local → Configure → Polling
   and inverter detection**.
2. Change **Control mode** to **Full Control**.
3. Open the inverter's entity list.
4. Enable only the specific setting you intend to test.
5. Record its current value before changing it, then make one small change and
   verify the result on the inverter or in the manufacturer's interface.

Do not enable all 118 entities at once. Most users need only a few settings,
and a compact entity list is easier to review safely.

## What write confirmation means

Every exposed setting uses the documented Modbus multiple-register write. For
a shared register, EyeBond Local reads the current word and changes only the
bits owned by the selected setting. It then immediately reads the same register
back and accepts the operation only when the raw value matches.

This confirms that the inverter stored the requested register value. It does
not prove that a setting is appropriate for your battery, grid rules, or site.
The inverter and installer requirements remain the authority for safe limits.

Setting values are refreshed in small rotating groups so normal power and
energy telemetry is not delayed by the large settings surface. After a reload
or reconnect, some disabled control entities may remain unavailable until their
group has been read again.

## Intentionally unavailable operations

EyeBond Local does not expose factory reset, production-test, EEPROM
initialization, calibration, BMS-owned live words, parallel-cluster addressing,
ambiguous register fields, or grid-code/certification curves. Those operations
are either destructive, owned by another controller, insufficiently documented,
or unsafe to present as ordinary Home Assistant controls.

If a write is rejected, unchanged, or produces unexpected behavior, stop
testing that setting and create a [Support Archive](SUPPORT_ARCHIVE.md). Include
the entity name, old value, requested value, and what the inverter displayed.

