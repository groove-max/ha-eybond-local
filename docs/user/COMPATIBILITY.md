# SmartESS, SmartValue and EyeBond inverter compatibility

[Українська](COMPATIBILITY.uk.md)

EyeBond Local connects to an **EyeBond-compatible collector** and identifies
the inverter protocol behind it. Using SmartESS, SmartValue or DESSMonitor is a
useful clue, not proof that every device in that app is supported.

## Check these three things

1. **Collector:** model/firmware and whether it supports a local connection.
2. **Inverter:** commercial model, rated power/voltage and firmware, not just brand.
3. **Confirmed capabilities:** readings and controls are separate; a working
   sensor does not prove that a similarly named setting can be written.

The [inverter model catalog](../generated/INVERTER_MODEL_CATALOG.generated.md)
records evidence, limitations and known variants. It includes Anenji, PowMr,
Sandisolar, MUST, Victor, Kevolt and other devices at different support levels.
Generic protocol support also exists where an exact commercial model is unknown.

## What the support levels mean

| Evidence | What it means for you |
|---|---|
| Confirmed readings | Valid local telemetry has been observed for that recorded variant. |
| Confirmed controls | Controls have supporting evidence; read the notes for the tested subset. |
| Partial / experimental | Some readings or settings are missing, unverified or require opt-in. |
| Research / unknown | Do not assume that a working driver or local controls exist yet. |

Not every unit sold under one model name has the same firmware or register map.
Likewise `VMII-NXPW5KW` is a shared PI30 firmware identity, not a unique brand
or a reliable power rating. The collector and inverter are different devices.

## If your model is not listed

Try normal discovery and check the identified driver and live readings. Do not
select an unrelated profile just because its brand is similar. Create a
[Support Archive](SUPPORT_ARCHIVE.md) if detection fails or data is implausible.
For missing cloud settings on an otherwise working inverter,
[Write Controls learning](LEARNING.md) may provide locally confirmed additions.

## No factory collector?

[ESP EyeBond Collector](https://github.com/groove-max/esp-eybond-collector) is a
community ESP8266/ESP32 bridge for supported wired inverter protocols. Check
its own electrical/interface requirements. It does not turn an arbitrary
inverter into an EyeBond device or add vendor-cloud access.

## Scope of the website

The catalog follows current main's reviewed records, not a promise that every
entry works on the latest tagged beta. [Choose the appropriate build](INSTALLATION.md).
Cloud Bridge 0.4 remains separate development; it is not advertised here as
an available solution to all cloud disconnections.
