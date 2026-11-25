# Anern FGI-S5000: register evidence and coverage

The tested collector was an [ESP EyeBond Collector](https://github.com/groove-max/esp-eybond-collector), not a factory logger. The [FGI-S5000 build guide](https://github.com/groove-max/esp-eybond-collector/blob/main/docs/integrations/anern-fgi-s5000/README.md) documents the socket orientation, MAX3232 wiring and ESP32-C3 SuperMini preset used for the hardware evidence below.

The [address index](fgi_5ku_registers.csv) accounts for **all 1,030 separate register addresses** in the [INV-Modbus V1.20 family workbook](https://github.com/pr0ma/icaura/blob/41a78f40d71ffc473c9915a1ba1fab295dfa50f9/INV-ModbusV1.20.xlsx). Filter by worksheet, FC03 evidence, runtime key or write policy. It contains no serial numbers, raw private capture, credentials or factory parameter values. A blank runtime key means deliberately not exposed, not an unimplemented promise.

The observed device uses the **Wi-Fi stick socket**, RS-232, 9600 8N1, RTU slave 1, not the battery BMS socket. HA verified FC4 forwarding using the generic catalog route. The family fingerprint combines version words at 0x0010/11 (1, 7), model words at 0x0038/39 (0, 0x20) and software word 0x0040 (0x0100). Neither the collector PN nor the single model word establishes a commercial identity. New revisions may need different anchors. V1.20 is a document version.

All five anchor values are required; no lookup priority is assigned to this model. The extensible Modbus catalog bounds each individual identity read, not the sum of successful reads across different maps. A hard caller deadline (e.g. during onboarding) still returns an incomplete scan. Runtime resolves the finite decision tree on the same owned session; no identity evidence survives a reconnect. Keep FC03 reads at 0x0038 (two words) and 0x0040 (one word) separate: combining them into one nine-word request returned exception 04 on this hardware.

| Part of the map | Policy |
| --- | --- |
| Summary, AC, battery, PV1 | Four FC03 blocks per normal refresh. Signed power is retained; a negative grid reading alone is not proof of export. |
| Work mode, charger, DC bus, fans, status, counters | One additional rotating block per refresh, shared with the settings pages. High word first for 32-bit energy; one raw count is 10 Wh. Do not substitute the coarse summary counters. |
| User settings (0x4100–414C) | All 77 words were read, including reserved and command words. Expose useful readbacks; only allow listed one-word FC16 operations. The 24 V bench returned 27.6/27.2 V bulk/float; do not import limits from 48 V siblings. |
| Battery management | Measurements require a linked status in **the same FC03 reply**; 0x7FFF is not a voltage, temperature or SOC. On this unit 0x01A3 returned exception 04; no runtime block crosses that boundary. No battery/BMS in the validation setup. |
| SOC, temperature, schedules | Summary SOC and thermistor values with disputed scale are diagnostic raw fields. 0xFFF1/FFF2 timer sentinels are not minutes or hours. The separate battery percentage is reported by the inverter, not independent confirmation of charge. |
| Other family hardware / service | Reading a register does not establish that a generator, PV2 or grid export is present. Factory, calibration, administrator and OTA pages are outside this user-settings map. Model/LCD configuration is opt-in, restricted to standby/off and was not exercised on the bench. |

**What was verified.** The 24 November serial capture contains 691 distinct CRC-verified words, 13 addresses seen only in rejected requests and 326 not queried. A block exception is not proof each address in it is invalid. On 25 November **every selected polling block** was read through the ESP32-C3 and HA with the inverter connected only to utility. Grid and output were both 232.8 V at 49.99 Hz with 0 W output. PV, battery, non-zero load and actual power-flow direction were not validated physically. The sanitized replay accompanies the driver tests; the BMS identifier slot is redacted.

**Write coverage.** There are 74 HA controls over 72 distinct settings, including separate Disable and Unlimited commands for the secondary output timer. Of the other five UserSet words, 0x411D and 0x4133 acknowledge an alarm and factory-reset rather than store a user setting, 0x4126 is reserved, 0x4143 is logger-owned status, and 0x4148 is documented only for VML1K2. Do not use the repeated decimal address for 0x414C in the workbook: its preceding word is a separate SOC setting. The ambiguous shutdown override is unverified.

The [write replay](../../tests/fixtures/fgi_5ku_write_readbacks.json) distinguishes FC16 acknowledgement from a changed FC03 value and a separately confirmed restoration. The first five toggles were backlight, mute, display return, grid warning and page keys. Subsequent trials cover charging, priorities, voltage and SOC thresholds, secondary output duration and operating settings. A verified word is not evidence for every choice, actual battery charging, timed switching or grid export. Several generator, schedule and feed settings echoed the write but retained the old value. These remain **untested**, not successful.

In HA, use **Configure → Runtime settings → Control mode → Full control** for the documented-only settings on this inverter. This changes HA's exposure policy, not the inverter's operating mode. Commissioning, clock and high-impact entities are initially disabled in the entity registry. Auto exposes only independently verified controls when the driver detection confidence is high.

The replay contains 66 trials (52 changed as requested; 14 did not). Including the earlier five display tests, 43 of the 74 HA operations have a confirmed change/restoration; 31 are documented-only. For 24 V setpoints the profile offers the bench-observed windows, not claims about firmware limits: for example, bulk 27.3–28.4 V, float 26.5–27.5 V and warning 22.0–25.0 V. The limit for a given installation can depend on chemistry and the other setpoints. Raw code 36 for maximum discharge current is **not** evidence of a 36 A setting.

On startup (and after writes with known related settings), five short pages populate the controls. Normal cycles still poll four telemetry blocks and **one** rotating page. Writes use one FC16 word followed by an independent FC03 after 1.3 s. RTC seconds alone may advance across the readback. 0xFFF1/FFF2 are never shown as time or current magnitudes. At 0x4142 this unit reported raw code 36, outside the documented 0–30 and Disabled encoding; keep the diagnostic code and inhibit writes until it can be decoded.

**Coupled settings.** Raising the low-battery warning from 24.0 to 25.0 V on the 24 V unit also raised the battery-to-utility threshold (24.8 → 25.0 V) and secondary cutoff (24.0 → 25.0 V). Lowering the warning did not lower either one. Each was independently restored and the full settings page re-read. Changing one threshold may therefore require reviewing the others; HA does not automatically overwrite the inverter's choices. No 48 V sibling limits have been copied into this profile.

**HA acceptance.** The normal full scan with the driver set to Auto selected
`fgi_5ku` on the mains-only unit and created a separate inverter device without
a manual protocol or register-map override. Independently, the catalog probe
over the same hardware completed ten identity requests in 10.59 s with the FGI
anchors at the end. After an observed network interruption the collector
reconnected and the same HA entities reported successive grid readings of
233.3, 233.8, 236.7 and 235.7 V without manual polling. The slower blocks
also populated DC bus voltage, charger bulk/float voltage and energy counters.
This is evidence of automatic recovery and polling in this test, not of
long-term Wi-Fi stability or of operation on PV or battery.

On the same bench in Full control, the HA registry contained all 74 control
entities. A real `number.set_value` call changed bulk voltage from 27.6 to
27.7 V and back; independent reads confirmed both writes and all 71 non-clock
words subsequently matched the saved baseline. With the bench entities enabled,
64 had a known state, seven were inhibited (four commissioning controls, two
equalization controls for the selected lithium chemistry, and the unmapped
discharge-current code), one numeric duration was unset because the secondary
output was disabled, and the two unpressed action buttons had no timestamp.
