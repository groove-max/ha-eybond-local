# Changelog

All notable changes to this project are documented in this file.

The format is inspired by Keep a Changelog, with one practical rule for this repository:
the GitHub release body should be rendered from the matching version section here.

## [0.3.0-beta.1] - 2026-07-10

### Added

- Added a **catalog-driven generic Modbus driver**: supporting a new Modbus inverter family
  is now a declarative device pack (identity anchors + register schema JSON) instead of a
  Python driver. Detection uses plausibility anchors (device-type codes, rated power, value
  envelopes) so one pack covers a whole family, and electrical variants (24 V vs 48 V) never
  fork the map.
- Added four inverter families from vendor protocol documents. These are
  **datasheet-based and not yet confirmed on hardware** — telemetry should work out of the
  box, and we would love a support package from early adopters:
  - **Sandi Aohai FSA** (read-only telemetry).
  - **Growatt SPF** off-grid series (telemetry + controls).
  - **Deye single-phase LV storage hybrids** (SUN-3/3.6/5/6K-SG04LP1 class; telemetry +
    controls). String inverters and microinverters share the wire protocol but not the
    register meanings, so detection strictly gates on the storage device type.
  - **Solis / Ginlong storage hybrids** speaking the ESINV energy-storage protocol
    (read-only telemetry; the public protocol document ships without control registers).
- Added **untested controls** as a first-class support state: controls derived only from a
  vendor document and never corroborated by cloud evidence or hardware are hidden in the
  default control mode and appear only when the user explicitly selects **Full control**.
  Growatt SPF and Deye LV ship their control sets this way (priorities, charge-current limits,
  grid/generator charge enables, and similar settings).
- Added **model-specific control surfaces** for three EyeBond/SmartESS-collector inverters,
  cross-referenced from cloud evidence and vendor register maps:
  - **MUST PV/PH18** now exposes a control surface built from the SmartESS cloud
    device-settings catalog and the vendor 1.4.15 register map — output voltage/frequency,
    energy-use mode, grid-protection standard, charge-source priority, battery type, PV/grid/
    combined charge-current limits, discharge current, and the battery voltage windows. The
    20 controls the cloud exposes ship visible in the default control mode; datasheet-only
    settings the cloud does not expose (grid charging, the battery-equalization block) stay
    in Full control. Every control reads back its current value. Hardware write confirmation
    is still pending a tester report, and the inverter validates its own writes (a locked or
    absent register self-disables).
  - **Aninerel ANL-4200T-24L-W-PRO** binds a model-specific full-control surface instead of
    the read-only family fallback; a tester confirmed writes on hardware, so the
    family-proven SMG settings are visible in the default control mode. Battery-voltage
    windows are left wide because this is a 24 V unit and the family templates carry 48 V
    windows, so the inverter's own validation is the authority.
  - **SMG family 4200 variant** (a new SMG model) ships controls graduated from a full
    shadow-learning run that correlated the SmartESS cloud's writes to real registers
    (input/buzzer/LCD/boot mode, output voltage/frequency, and mains/off-grid battery
    low-voltage protection).
  - **ANENJI ANJ-5KW-48V-WIFI** now matches as a full SMG layout-11 model
    (model code `0x7901`, rated power 5 kW) and exposes the SmartESS-confirmed control
    surface from a complete shadow-learning run.
  - **Anenji ANJ-11KW-48V-WIFI-P** adds the missing secondary-output and secondary-charging
    controls confirmed by a user shadow-learning package: OP2 overload alarm, secondary
    charging priority, float-charge wait time, maximum discharge-current protection, and OP1
    off-grid SOC protection.
- Added a **raw AT wire-evidence probe** to support archives for `at_text` collectors: when
  detection fails on a raw-serial ASCII collector, the archive now records a bounded PI30 and
  G-ASCII read sweep with per-command request/response bytes, and the collector diagnostics
  surface the raw request/response counters from the AT connection.
- Added a **transactional inverter-link baud sweep** to the deep scan for ESP EyeBond
  Collector bridges: when the inverter stays silent on the configured UART speed, the scan
  walks the candidate speeds a protocol family declares, probing only the drivers expected at
  each speed, and always restores the original speed. This finds e.g. SRNE/MUST-class units
  on 19200 behind a bridge provisioned at 9600 (ESP32/ESP8266 bridges only; bk72xx UARTs are
  fixed at runtime).
- Added Modbus **input-register (FC 0x04) support** to the shared Modbus core, plus new
  decode features packs needed: per-spec raw offset (Deye-style `(raw-1000)*0.1`
  temperatures), low-word-first 32-bit counters, and divisor-implied precision.

### Changed

- Detection internals were consolidated around single owners: one anchor-matching semantic,
  one resolution engine (the compiled decision tree), one decision-DAG walker shared by all
  catalog probers, and one register decoder shared by SMG, SmartESS, and the generic packs.
  Along the way a real mis-detection was fixed: integral-float signature values
  (`220.0` vs `220`) could push a known LVYUAN unit into the read-only family fallback.
- **LVYUAN TY-SIC-3.6KBE-W1** now ships its full cloud-confirmed G-ASCII control surface as
  tested/default-visible instead of keeping most controls hidden as untested.
- The deep-scan silence verdict is now grounded in transport-observed responses instead of
  driver-reported outcomes, so "no supported device" and "device never answered" are
  distinguished honestly.
- Switching the control mode now reloads the config entry, so control entities that only
  materialize at platform setup (the untested/Full-control set) appear and disappear
  immediately instead of after a manual restart.
- Support packages are now uniformly share-safe: every archive member masks long numeric
  identifiers (collector PN, serial numbers) with the same rule, including identifiers
  embedded in hex payload dumps; replay fixtures keep an anonymized twin, and the manifest
  always references the real archive file.

### Fixed

- **MUST PV/PH18 battery power vs current**: register 25274 is the battery *current* (A),
  which a third-party map mislabeled "Battery_Load"; it is now exposed as Battery Current,
  and the vendor's actual "Batt power" register 25273 is exposed as Battery Power (W).
- **MUST PV/PH18 control read-back**: the settings blocks were widened to gap-free register
  ranges so the inverter no longer rejects them, and every control decodes its current value
  (previously the wide blocks spanned absent registers and left the controls blank).
- Pack control entities are registered enabled by default, so a device that exposes its pack
  controls no longer needs each entity enabled by hand; the enabled-defaults self-heal
  re-enables entities that a previous version had left disabled.
- **ESP EyeBond Collector bridges**: the Home Assistant callback endpoint written into the
  collector is now always the entry's own listener port (a proxy-template port could
  previously leak into the collector and strand it), and bridge entries always resolve to the
  framed transport profile — both at onboarding and through runtime reconciliation — fixing
  scans that silently probed the wrong protocol after SmartESS-style metadata answers.
  Endpoint changes pair best with esp-eybond-collector **v0.1.8**, which defers the endpoint
  apply until the acknowledgement is flushed.
- **Collector transport lifecycle**: a replacing collector session can no longer tear down
  its successor (previously a live collector could "vanish" until it redialed); teardown no
  longer inherits a dead peer's TCP timeout or a swallowed task cancellation (both could hang
  Home Assistant shutdown); unidentified callback sockets are parked under a watcher instead
  of lingering unwatched where a dead socket blocked same-IP routing; and the byte-shape
  sniff defers to registered per-PN session owners.
- **Phantom daily grid import on SMG-class hybrids**: the power-flow split now trusts the
  charger's own PV measurement over headroom derived from an under-reading `pv_power`
  register. A PV-only system no longer accrues fake grid-import energy.
- Onboarding fixes from a deep review pass: the scan aggregator no longer cancels an
  extendable deadline based on a stale snapshot, probe logs survive scan errors, a slow first
  refresh no longer blocks entry setup, restricted re-scans no longer spend probes on
  excluded drivers, and explicit `decimals: 0` is honored in register decoding.

## [0.2.0] - 2026-07-02

### Added

- Added first-class support for the community **ESP EyeBond Collector** virtual bridge
  (firmware for inverters without a factory collector). The bridge is detected from the
  collector hardware-version token read through FC=2 parameter 6
  (`collector_hardware_version = esp-collector/<version>/<platform...>`); detection never uses
  the collector PN, devcode, or factory firmware-version string, and no extra AT probe is sent
  during detection. A detected bridge is shown as an honest **ESP EyeBond Collector** device
  (manufacturer, model, firmware version, and project link) and its cloud-only actions —
  device learning / shadow learning, proxy capture, and cloud-assist actions — are hidden
  unless a future firmware explicitly advertises cloud capabilities. Its collector operation
  mode is fixed to **Home Assistant only** (the Cloud + HA choice is not shown), and reverse
  discovery stays on so it can reconnect. Local actions (runtime settings, diagnostics, and
  Change Collector Wi-Fi) stay fully available. Already-onboarded bridges keep working from
  their persisted bridge identity; fresh bridge onboarding requires esp-eybond-collector
  firmware that emits the `esp-collector/<version>/<platform...>` token.
- Added an adaptive sensor refresh scheduler. New entries default to
  **Automatic** refresh, where EyeBond Local chooses the next poll interval from
  the observed device response time and protocol-specific limits; **Manual**
  refresh keeps the existing fixed-interval behavior for upgraded entries and
  users who want a fixed cadence.
- Added collector diagnostics for poll mode, current/next interval, poll
  duration, utilization, recommended interval, and scheduler delay.
- Added poll-context diagnostics so collector-only, detection, and runtime
  polling cycles are visible separately.
- Added an offline device identification catalog: inverters are now identified by a
  deterministic register fingerprint (protocol layout + model code + rated power) with explicit
  support tiers, and the catalog — not heuristics — decides which schema and controls apply.
- Added a detection summary step to onboarding: after the scan you see the identified model,
  its support tier (full / partial / not recognized), and what to do next, before the device
  is created.
- Added canonical power-flow telemetry for the SRNE and MUST PV PH18 drivers: `pv_power`
  (SRNE: PV1+PV2 sum, MUST: solar-charger power), signed `battery_power` (SRNE register 270,
  MUST register 25274), and the six `*_to_*_power` flow-split sensors now populate from
  registers these drivers already poll, so the power-flow card renders fully for both
  families. SRNE has no grid-power register and MUST has no battery-SOC register, so those
  two slots stay empty until the registers are identified.
- Added a guided control-discovery wizard ("Add controls (device learning)") for partially
  supported and unrecognized inverters: one linear flow (consent → vendor-app sign-in → progress
  → review → apply) replaces the old technical action menu. Sessions are fail-closed: the
  collector is always restored even when a run fails.
- Added read-sensor learning: during a learning session the integration also captures which
  registers the cloud reads, binds cloud sensor labels to registers by value correlation and
  enum matching, and can apply the learned read sensors even when no controls were selected.
- Added new supported models from community-donated captures: **Anenji 6200 (dual output)** —
  full support including second-output telemetry (power, apparent power, load, voltage,
  cut-off SOC, overload threshold) and an **Output 2 on/off switch** — and **Anenji 6200**
  (single output, full support with the SMG control set).
- Added more supported models: **Anenji ANJ-4000W-24V** (full SMG telemetry), and read-only
  telemetry support for **MUST PV18**, **SRNE-compatible Modbus** inverters, and **LVYUAN**
  units on the new **SmartValue / EyeBond G-ASCII** protocol. The full, always-current device
  matrix is the generated [inverter model catalog](docs/generated/INVERTER_MODEL_CATALOG.generated.md).
- Added collector **callback identity routing**: collector callbacks are routed by collector
  identity (PN) instead of peer IP, so several collectors behind one router/NAT are tracked
  separately, and a collector callback-identity diagnostic sensor surfaces the routing state.
- Added bit-level write capabilities: controls that own a single bit of a shared register are
  written read-modify-write, preserving the other bits (this enables the Output 2 switch).
- Added a memory guard to learning: on memory-tight hosts the scan refuses to start
  (`insufficient_memory`) instead of risking an out-of-memory crash.
- Added a contribution toolchain for donated captures: contribution record builder, vetting
  tool, and a GitHub issue template for sharing support packages.
- Added a validation toggle (`EYBOND_FORCE_UNSUPPORTED=1` env var, or a
  `force_unsupported.flag` sentinel file in the HA config data dir) that treats every model as
  unsupported so the learning flow can be exercised on a fully supported device.
- Added a developer-directed **diagnostic command runner** — the
  `eybond_local.run_diagnostic_commands` action and an "Run diagnostic commands" options screen
  (shown only with Home Assistant Advanced Mode) — that runs a small read/`write`/`write_bit`/
  `ascii` scenario against the inverter over the existing collector connection, for adding or
  debugging device support. It never changes config-entry settings, runs one scenario per entry
  at a time, and requires an explicit `confirm_write` before any scenario that writes to the
  device. Results are saved locally; the shareable copy has known identifiers redacted.

### Changed

- Deep scan is now a real extended detection path instead of a lightly longer quick scan:
  it keeps structured evidence for each target, does not stop a target batch after the first
  matched inverter, uses a distinct extended timeout budget, and surfaces detection timeouts
  separately from ordinary collector-only results.
- The deep-scan time budget is now derived from the registered drivers' own signature and
  probe budgets instead of a hand-maintained constant that was smaller than the worst-case
  driver sweep, and each connected target gets its own sweep deadline so one slow target
  cannot starve the identification of another. Together these remove the main cause of
  connected collectors finishing as "detection ran out of time".
- Driver detection now uses the protocol metadata a collector reports (mapped through the
  protocol catalog, not a hardcoded family list) to probe the matching local driver first.
  Previously the probe order fell back to the registry order when the signature pre-pass
  did not match, so a PI30-family inverter could burn most of the deep-scan budget on
  Modbus drivers before its own driver was ever tried.
- Deep detection now records a per-driver probe log (driver, elapsed time, outcome) into
  the detection evidence and the created entry (`detection_probe_log`), so real
  installations produce the data needed to validate and tune the per-driver probe budgets
  instead of guessing.
- The driver-choice step was made human-readable: options are now
  "model — driver (recommended)" with the measured identification time per candidate,
  the raw probe-route digits are gone (the device address is shown only when two
  candidates differ by nothing else), and the summary explains that protocols can
  differ in polling speed. Candidates show the localized driver display names
  ("SmartESS 0925 / Modbus", "SMG / Modbus", "PI30") instead of internal keys, and the
  missing English fallback label for the SmartESS-local driver in the driver selector
  was added.
- After a deep scan the repeat action is labeled "Repeat deep scan" (repeating always
  re-ran the same scan mode, but the generic label made it look like a quick scan and
  sent users through the advanced menu to reach deep scan again).
- Scan targets whose probe was deliberately cancelled because another target already
  matched are no longer reported as detection timeouts: they carry the dedicated
  `cancelled_first_match_found` evidence status, keep what was learned about the
  candidate, and present by their actual state (collector replied / connected) instead
  of "detection ran out of time".
- The scan-results screen lets you add a found device directly: the device list and the
  follow-up actions (refresh, advanced setup) live in one selector, replacing the extra
  "Add detected device" menu hop and its separate selection step.
- Deep scan now starts immediately when the selected interface has a known, normally
  sized network; the intermediate confirmation step remains only where the user has a
  real decision to make (a large subnet, or an unknown network).
- The deep-scan identification headroom now sits on top of the discovery budget instead
  of sharing one ceiling with it, so scanning a /16 network (whose discovery alone takes
  ~14 minutes) no longer consumes the identification budget.
- Collectors owned by existing entries are now visible in the scan results as
  "Already added" (previously the unprobed marker was filtered out of the list, so the
  summary claimed zero configured devices).
- The scan-results list was decluttered: the status chip is not repeated in the details
  (no more "SmartESS hint — ... SmartESS metadata ... collector connected"), the
  "Unconfirmed inverter" filler is gone, serial numbers are shown only when known, and
  confidence is lowercased mid-line.
- The deep-scan time budget now follows the discovered work instead of being fixed
  upfront: every collector that connects and is admitted for identification extends the
  shared scan deadline by one full driver-sweep budget (bounded by a 15-minute runaway
  ceiling). A site with many inverters gets one sweep per collector instead of all of
  them starving on a budget sized for one or two.
- Scans no longer probe collectors that already belong to a configured entry: probing
  stole the collector's callback session from the running entry and burned the shared
  scan time budget on devices that cannot be added again anyway. Such collectors are
  listed as "Already added" without being touched.
- A collector that dials back in now triggers an immediate refresh when the entry is not
  bound, instead of idling until the next scheduled poll — after failed detection cycles
  that could be more than a minute away. Together with the next change this removes most
  of the "one update, then unavailable for a minute" churn right after adding a device.
- Consecutive failed refresh cycles no longer re-run the full (slow) collector AT metadata
  sweep every time: the caches are invalidated once per outage, and collector liveness is
  proven with the cheap framed query when available. This also stops the failed-cycle
  duration — and therefore the retry backoff derived from it — from being inflated by our
  own metadata reads.
- Deep scan now keeps every successful local driver/protocol probe for the same device.
  When one inverter responds through multiple protocols, onboarding shows a driver choice
  step with the successful candidates instead of silently keeping only the first match.
  Choosing an alternative keeps the full probe metadata (SmartESS details included) and
  re-runs the confirm-time detail refresh with the chosen driver, and if the deep-scan
  time budget runs out mid-probe the candidates found so far are kept instead of being
  discarded with a bare timeout.
- New entries persist detection evidence (`detection_depth`, `detection_status`,
  `detected_probe_route`, and the candidate driver list when more than one protocol
  matched) so the support package can explain how a device was onboarded.
- Onboarding UX overhaul: the welcome form is gone, the happy path is decluttered, cloud
  assist is an explicit optional choice (never an interstitial), developer tooling is
  removed from the options flow, and long wall-of-text screens were rewritten into short
  actionable guidance (en/ru/uk).
- Unknown-but-SMG-family inverters now onboard at the partial tier with base read sensors and
  a clear pointer to device learning, instead of being rejected with
  "no supported driver matched".
- Inverter-communication loss during detection now surfaces as "inverter link down / retry"
  instead of a false "no supported driver matched".
- Startup is lighter: metadata catalogs are warmed off the event loop, removing blocking file
  reads during driver detection (matters on slow or throttled hosts).
- The runtime settings flow now hides the fixed poll interval while Automatic
  refresh is selected and shows it only for Manual refresh.
- High-utilization warnings now point Manual-mode users either to a larger
  interval or to Automatic refresh.
- Automatic refresh no longer learns from collector-only, offline, or inverter
  detection cycles, so a missing or unsupported inverter does not permanently
  inflate the normal runtime poll interval.

### Fixed

- The poll-debugging diagnostic sensors (Poll Phase Breakdown, Refresh Phase Breakdown,
  Slowest Driver Requests) are disabled by default — enable them per collector when
  chasing a slow poll — and all of them live on the collector device (the refresh
  breakdown and slow-requests sensors previously landed on the inverter device).
- Added "Refresh Phase Breakdown" and "Slowest Driver Requests" diagnostic sensors: the
  runtime refresh reports where its time went (collector metadata, driver detection,
  driver read, snapshot build) and the SmartESS-local driver reports its five slowest
  register requests per cycle with outcomes.
- Added a "Poll Phase Breakdown" diagnostic sensor: each poll cycle reports where its
  wall-clock time actually went (network reconcile, session profile, runtime refresh,
  snapshot profile, endpoint reconcile), so a slow cycle is explained by a sensor read
  instead of a packet capture.
- The Collector Protocol Asset ID and Collector Devcode diagnostics no longer flip
  between two values every cycle. Parameter 14 on some collectors returns a composite
  serial-protocol config string ("02FF,0,0,#0#") — the id is now parsed from its first
  field, and an asset id is claimed from parameter 14 only when the protocol catalog
  knows it, so it cannot fight the asset id the bound driver reports. The devcode
  diagnostic now shows the collector's stable heartbeat devcode instead of the devcode
  of whatever frame happened to arrive last.
- Whether a collector answers the AT metadata channel at all is now a learned per-device
  fact, like unsupported inverter commands: framed collectors tunnel AT via raw
  passthrough and only some firmwares support it, so after two evidence-gated failures
  the channel is skipped entirely (persisted as `collector:at_metadata`, cleared by the
  same "Re-check Supported Commands" button). Collectors where AT-over-passthrough works
  (Wi-Fi scan, signal metadata) keep it.
- Fixed the actual cause of the stable ~60-second poll cycles on EyeBond collectors with
  a framed callback session: the collector AT-metadata sweep (12 commands: DTUPN, ATVER,
  WFSS, ...) ran against an AT channel that never answers, burning a full request timeout
  per command, and the empty result was never cached — so the sweep repeated every
  cycle. The sweep now aborts after the first timeout (a dead AT link times out for
  every command), and its retry cadence is keyed on attempts instead of successful
  results. The refresh-phase diagnostic additionally splits collector metadata into its
  framed (FC) and AT parts.
- The SmartESS-local (Modbus) driver applies the same unsupported-command memory to its
  register blocks: a bulk read the inverter rejects or ignores was retried every cycle
  (block failures never marked the block as read), spending most of a ~60-second poll on
  consecutive request timeouts while the wire carried only a few seconds of real traffic.
  Rejected blocks and dead fallback registers are now remembered per device
  (`block:config`, `register:5005`, ...), the capability values keep coming from the
  cheap single-register fallbacks, and the same "Re-check Supported Commands" button
  clears the memory.
- The ASCII drivers (PI30, PI18, EyeBond G-ASCII) no longer re-send unsupported commands
  on every poll cycle. On inverters that only answer the core command set, every optional
  or energy command burned a full request timeout each cycle, turning a ~2-second poll
  into a ~60-second poll. A command that fails twice — in cycles where the device
  answered something else, so a link outage never counts as evidence — is marked
  unsupported for this device permanently: the learned set is persisted into the config
  entry (`driver_unsupported_commands`), survives restarts and reconnects, and is shown
  as a diagnostic value. A new "Re-check Supported Commands" diagnostic button clears the
  learned set and probes everything again (for example after an inverter firmware
  update).
- One collector seen through two scan sources (the configured-IP marker plus a
  PN-carrying callback-session result) is now collapsed into a single scan line; the
  PN-carrying duplicate wins so the line keeps the identity.
- Collector callbacks that no config entry owns are now parked (held open passively,
  bounded and with a TTL) instead of being closed after classification. Closing made the
  collector firmware redial within seconds, producing a permanent connect/close loop for
  collectors whose entry was removed; a parked callback also stays instantly claimable by
  a scan or a newly added entry, including its already-received identity bytes.
- A collector whose management link answers but whose inverter sends no heartbeat is no
  longer reported as offline. The runtime now separates the two layers: collector-level
  sensors keep updating, the state becomes `driver_unbound` with the new
  `inverter_heartbeat_missing` code (instead of the misleading `collector_heartbeat_timeout`),
  and a bound inverter still goes through the normal reconnect/recovery path. This fixes the
  esp-collector bridge appearing stuck as "waiting for collector" after a power cycle even
  though its TCP session was live.
- Stale UDP discovery details (`collector_udp_reply`, `collector_udp_reply_from`) are now
  dropped when the collector goes offline instead of being shown from the previous session.
- The manual-mode high-utilization warning no longer misfires after a device outage. Cycles
  that bound the driver or recovered the collector connection measure that recovery work,
  not the normal poll cost, so they are now excluded from the warning streak, from the
  poll-duration statistics (average/max/recent sensors), and from the adaptive scheduler's
  learning samples — a device coming back online no longer inflates the reported
  utilization or the recommended minimum interval. The warning is also dismissed
  automatically once polling stays within the configured interval again.
- Device learning now actually works on the partial / unrecognized tier it targets. Two
  coupled defects made it unusable there: those devices silently inherited the full controls
  profile (so overlay generation deduped against the wrong base), and the readiness check kept
  reporting `missing_effective_metadata_snapshot` because that tier never persists a snapshot.
- Changing the collector Wi-Fi no longer fails with
  "collector_listener_bind_failed … address in use" while the entry is loaded.
- Guided control discovery is more robust: closing the dialog mid-scan now still restores the
  collector route (fail-closed), a single bad cloud value can no longer abort a successful run,
  a successful-but-empty run is no longer shown as a failure, and re-running the wizard no
  longer shows the previous run's results.
- The Output 2 / bitmask switches no longer get permanently blocked when a transient read error
  happens while toggling them.
- Partially supported devices: the add flow now tells you the next step to unlock controls,
  and the learning consent dialog is correctly translated (ru/uk).
- Removed a harmless but noisy "Unable to remove unknown job listener" error on unload.
- Catalog entries are validated on load (support tier and its controls-profile invariant), so a
  malformed entry fails fast instead of silently degrading onboarding.
- Privacy: all real device identifiers (collector PNs, serials, credentials, network details)
  were replaced with synthetic stand-ins across tests and catalog provenance, and a guard test
  now fails the suite if a real-looking identifier enters the source tree.

### Performance

- Lighter coordinator refresh on resource-constrained hosts: metadata base-name resolution is
  cached instead of reading overlay files from disk every poll, the shadow-learning session
  state is no longer re-read from disk every refresh, and redundant per-poll Modbus reads on
  multi-output variants were removed.

### Docs

- Updated the README hardware table, device-learning guide, and setup walkthrough (en/uk) for
  the catalog-based identification, the guided learning wizard, and the new Anenji 6200 models.

## [0.2.0-beta.1] - 2026-05-12

### Beta Notice

- This is a prerelease for the 0.2.0 line; install it only when testing the new collector-first onboarding, local callback ownership, and collector operation mode flows.
- Keep a Support Archive before reporting collector matching, callback ownership, proxy capture, or inverter detection issues.

### Added

- Added a collector-first onboarding flow with collector network choice, optional Bluetooth Wi-Fi provisioning, scan-interface selection, and clearer quick-scan / deep-scan / manual-setup branching.
- Added separate collector device management with collector operation mode, Wi-Fi change, restart, proxy capture, and collector-scoped diagnostics.

### Changed

- EyeBond Local now presents collector and inverter devices as separate parts of one installation, with user-facing runtime settings centered on collector mode and control mode.
- User-facing setup and runtime copy now explain the two everyday collector modes, `Cloud + HA` and `HA only`, in plain user terms.
- Support artifacts now split collector, inverter, and integration roles so beta reports are easier to triage.

### Fixed

- Switching between `Auto` and `Full Control` now reloads the entity set correctly instead of leaving stale expert entities behind.
- Collector callback endpoint override is available again in `Full Control`, and hidden full-control entities are cleaned up correctly when returning to safer modes.
- Legacy EyeBond cloud endpoints now resolve with the legacy port default when only the host name is known.
- Legacy collector signal entities are cleaned up from existing installs instead of remaining visible after their defaults changed.
- Removed collector entries and Home Assistant Core restarts now close owner-specific callback sockets and avoid accepting orphan collector callbacks.
- Local onboarding no longer offers cloud assist before creating a local entry, and HA-only mode applies its callback binding silently.

### Docs

- Updated the English README around the new onboarding flow and collector modes, added a public collector-management guide, refreshed the docs index, and replaced outdated setup screenshots.

## [0.1.53] - 2026-04-25

### Added

- Collector diagnostics now track connection churn, dropped pending requests, disconnect reasons, and discovery restarts to make local write-path contention easier to diagnose.

### Changed

- Declarative SMG runtime gates now surface advisory warnings instead of locally hiding or hard-blocking controls; the inverter response and immediate readback confirmation are now the final authority for non-action writes.

### Fixed

- Non-action writes no longer report silent success when the refreshed value stays unchanged; EyeBond Local now raises an explicit `write_not_confirmed` error after readback.
- Temperature sensors backed by the affected PI18 and Anenji schemas now report `°C` instead of plain `C`.

### Docs

- Updated the English and Ukrainian READMEs plus the SMG support matrix to describe advisory runtime warnings, explicit readback confirmation, and the new collector contention diagnostics.

## [0.1.52] - 2026-04-23

### Added

- Added an explicit built-in `anenji_4200_protocol_1` SMG runtime path for classic protocol-1 hardware that matches the documented `device_type=0x3501`, `protocol_number=1`, and `rated_power=4200` anchors.
- Added documented `power_flow_status` decoding for classic SMG protocol-1 layouts, exposing diagnostic connection, battery, load, and charge-source states instead of leaving the raw register uninterpreted.
- Added documented classic protocol-1 fault/log support-capture coverage for `700..744`, so support archives retain a broader evidence window for SMG 6200 and the document-backed Anenji 4200 path.

### Changed

- SMG protocol-1 writable metadata is now layered through a real shared base plus model overlays, so the common path carries only the shared protocol-1 controls and presets while 6200-only extras stay model-scoped.
- The support workflow and support-bundle markers now treat explicit but still-unverified model-specific profiles separately from the read-only SMG family fallback, keeping the new Anenji 4200 path at partial support instead of labeling it as a generic fallback.

### Fixed

- SMG common protocol-1 schemas and probe metadata no longer leak 6200-only diagnostics like `341..343` and `max_discharge_current_protection` into other protocol-1 variants.
- The Anenji ANJ-11KW-48V-WIFI-P schema no longer depends on accidentally inherited low-DC measurement metadata after the SMG common/base cleanup.
- The project-wide support overview export now includes the new built-in Anenji 4200 protocol-1 runtime profile instead of silently omitting it from release documentation.
- Runtime control-mode labels in the options flow are now localized instead of falling back to hard-coded English labels.
- Re-adding the verified default SMG 6200 integration now restores the tested write controls that should be enabled by default, including the equalization settings and the previously verified battery threshold controls.
- `Sync Inverter Clock` no longer leaks into the verified default SMG 6200 runtime path; it now stays scoped to the Anenji ANJ-11KW-48V-WIFI-P model-specific tooling path.
- Pending onboarding no longer stalls when a saved manual or pending device keeps the default broadcast discovery target as `collector_ip`; the shared listener now aliases that placeholder to the real collector callback IP.

### Docs

- Updated the English and Ukrainian READMEs, the SMG support matrix, and the generated support overview to describe the new Anenji 4200 protocol-1 path, the stricter SMG common-vs-model layering, the verified default SMG runtime path wording, and the current protocol-1 read coverage more accurately.
- Expanded the English and Ukrainian READMEs and onboarding copy to explain what the Pending Device / `EyeBond Setup Pending` state means, what to expect from it, and which retry steps to use before opening a support issue.

## [0.1.51] - 2026-04-18

### Fixed

- Anenji inverter date/time sensors no longer stay unavailable on hardware that returns valid clock registers only when the optional inverter clock range is read as one contiguous block.

## [0.1.50] - 2026-04-18

### Added

- Added a separate deep-scan onboarding path that can probe the full selected IPv4 network from both the first setup step and the scan-results screen.
- Added BusyBox-compatible IPv4 interface parsing for Home Assistant OS, so deep-scan network size and broadcast metadata still resolve correctly when `ip -j` is unavailable.
- Added runtime-schema-aware entity selection for model-specific SMG variants, which restores Anenji PV1/PV2 and other variant-only entities when the detected runtime metadata differs from the generic driver defaults.

### Changed

- The Anenji ANJ-11KW-48V-WIFI-P model-specific write surface is now marked as tested on real hardware, so its validated controls can participate in normal high-confidence `auto` exposure.
- The setup wizard now distinguishes quick scan from deep scan explicitly, with scan-mode-aware hints, timing estimates, and follow-up actions.

### Fixed

- Quick scan now stays effectively broadcast-first by removing duplicate broadcast targets and shortening reverse-connection waits when no UDP reply was received.
- Deep scan no longer reports zero-address networks on BusyBox-based Home Assistant OS hosts and remains available from the results screen even when candidates were already found.
- The scan progress bar now publishes its first determinate update immediately instead of briefly jumping from an indeterminate-looking state.

### Docs

- Updated the English and Ukrainian READMEs, SMG support docs, and generated support overview to describe deep scan, the validated Anenji control surface, and the current onboarding fallback flow more accurately.

## [0.1.49] - 2026-04-17

### Added

- Added a dedicated SMG `family_fallback` runtime path with explicit read-only/unverified markers in the support workflow, support bundle, and exported support archive.
- Added broader built-in Anenji ANJ-11KW-48V-WIFI-P monitoring, including PV1/PV2 telemetry, inverter date/time readback, native PV day/total counters, and a `Sync Inverter Clock` tooling button.
- Added broader SMG read-side diagnostics for the verified default path, including `program_version`, `protocol_number`, `device_type`, `battery_type`, `warning_mask_i`, `dry_contact_mode`, and `automatic_mains_output_enabled`, with cautious hidden-by-default exposure for lower-value raw fields.

### Changed

- SMG writable metadata is now layered through shared family/base/default/model profiles with capability templates instead of one duplicated monolithic profile file.
- The Anenji profile is now shipped as a real 47-capability model-specific control surface, but those writes remain intentionally untested and stay out of normal `auto` exposure.
- Runtime metadata and support reporting now label internal runtime paths separately from commercial hardware names, so docs and exported reports are less misleading.
- The daily grid-export helper now stays available when signed or direct export power keys are present, even if `solar_feed_to_grid_enabled` is missing.

### Fixed

- The default SMG binding now stays limited to verified 6200-class hardware; other SMG-like power classes fall back to the read-only family path instead of inheriting the default write surface.
- Optional SMG probe diagnostics now backfill missing details during normal refresh, so the surviving probe-only sensors remain available after Home Assistant restarts.
- Placeholder all-zero SMG `device_name` values are now suppressed instead of surfacing as misleading identifiers on the verified SMG 6200 path.
- Local draft and SmartESS bridge generation now copy fully resolved profile JSON, so profile shims and layered metadata do not leak into generated local files.

### Docs

- Updated the README, Ukrainian README, SMG support docs, and generated runtime-profile reports to describe the verified default SMG path, the Anenji-specific path, and the read-only SMG family fallback more explicitly.
- Release docs and CLI examples now use the changelog-first flow with version placeholders instead of stale hard-coded historical tags.
- Removed the extra README card badge while keeping the companion card link in place.

## [0.1.48] - 2026-04-17

### Added

- Added optional cloud assist for onboarding and diagnostics, including reusable cloud-evidence export for one collector identity.
- Added JSON-first SmartESS protocol and model-binding catalogs plus imported SmartESS assets `0912`, `0921`, and `0925` for metadata ownership, diagnostics, and local draft tooling.
- Added SmartESS local collector helpers for collector query/set commands, protocol-id parsing from query `14`, and known-family metadata planning.
- Added read-only SMG model coverage and wider support-archive capture windows for Anenji ANJ-11KW-48V-WIFI-P / Protocol 3-10 devices.

### Changed

- `Create support archive` is now the main diagnostics flow and can include saved cloud evidence automatically or refresh it inline before the ZIP is built.
- Runtime diagnostics, support export, and local draft tooling now resolve effective profile/register-schema ownership from saved or live SmartESS metadata hints, so imported SmartESS assets can be used before a native SmartESS runtime driver exists.
- PI30 default metadata now uses the canonical SmartESS `0925` compatibility paths, while user-facing naming presents raw `VMII-NXPW5KW` devices as PowMr 4.2kW.
- Advanced metadata tools now focus on raw JSON export plus SmartESS draft and bridge generation instead of duplicating routine archive and reload actions.

### Fixed

- Metadata cache priming now also warms catalog-driven metadata, avoiding blocking file reads when Home Assistant starts or reloads local overrides.
- Support archives now store matching cloud evidence only once inside the ZIP under `evidence/cloud_evidence.json`.
- Support-archive raw register capture now follows the effective schema name, so model-specific SMG evidence windows are not dropped for variant overlays.
- External relative metadata overrides can now fall back to built-in parent profile and schema files when the local parent file is missing.

### Docs

- Public docs now explain cloud evidence, inline archive refresh, and the retention behavior of saved cloud-evidence files.
- Public docs now call out PowMr 4.2kW and Sandisolar SD-HYM-4862HWP as the currently verified commercial examples for the PI30 and SMG families.

## [0.1.43] - 2026-04-15

### Added

- First public GitHub release of EyeBond Local.
- Built-in local support for SMG / Modbus and PI30-family collectors, plus experimental PI18 replay coverage.
- Support Archive export workflow for unsupported or partially supported inverters.
- SMG writable and readback coverage for registers `341`, `342`, and `343`.

### Changed

- Config-flow runtime copy now loads from private `flow_translations/` bundles while Home Assistant-validated translation files remain Hassfest-compatible.
- Public release validation now passes HACS Validation, Hassfest, and the repository quality gate in GitHub Actions.

### Fixed

- Publication blockers around config schema exposure, manifest ordering, and unsupported translation key placement.

### Docs

- Public README and release metadata were aligned for the first published release.
