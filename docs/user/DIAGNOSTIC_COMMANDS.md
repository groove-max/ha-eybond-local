# Diagnostic Commands

Diagnostic commands are an advanced support tool for running a short,
developer-provided scenario through the collector connection that already
belongs to an EyeBond Local entry.

Most users never need this tool. Start with a [Support Archive](SUPPORT_ARCHIVE.md)
and use diagnostic commands only when a developer asks for a specific scenario.

## Safety boundary

### Collector information over Bluetooth

**Diagnostics and service tools → Inspect collector over Bluetooth**
is a separate, query-only tool. It does not require a working TCP callback.
It uses Home Assistant's connectable Bluetooth discovery: either a local
adapter or an ESPHome Bluetooth Proxy with active connections enabled must
be within radio range of the collector. Merely reaching HA over the internet
does not provide Bluetooth access to a remote collector.

Close the vendor app's Bluetooth connection first. Select the collector
explicitly, keep **No line ending** unless support asks for CRLF, and choose
**Read collector information**. Refreshing the list or reopening the result does not
send queries. The tool attempts only `AT+DTUPN?`, `AT+FWVER?`, `AT+ATVER?` and
`AT+CLDSRVHOST1?`. If the first PN query was sent but timed out and all three
other metadata queries succeeded, it rechecks PN **once**, on the same connection.
Both attempts remain in the result. The recheck requires a notification response,
not a cached GATT read, and never overrides an invalid or mismatching identity.
There are at most five queries, each with the same four-second timeout; no
reconnection or retry loop. It never provisions Wi-Fi, changes an endpoint,
restarts a device or writes inverter settings. No automatic alternate command
or line-ending search is performed. Only the vendor GATT layout is supported.

An incomplete result is useful evidence, not a successful connection test.
The PN returned by the collector must match the entry; a mismatch stops further
queries. Missing firmware, port or protocol values are not filled with defaults.
Reading an endpoint does **not** verify TCP callback or endpoint restoration.
Closing the dialog cancels the attempt and completes connection cleanup.

On newer main builds, the JSON records connection readiness and per-query
transport evidence: whether the GATT write returned, notification counts/byte
counts, candidate reply lengths, GATT read results/errors, the response source,
and elapsed time. Ignored fragments are counted by category (placeholder,
query echo, binary/control bytes, unmatched text), separately for notifications
and reads; these categories do not identify the source of unsolicited traffic.
This distinguishes a write-stage timeout from silence,
unrelated traffic or an incomplete reply. A completed GATT write does not prove
that firmware processed the command; received bytes do not prove identity or
freshness. Raw payloads and backend exception messages are not added to this
evidence. A late response to the first PN query cannot be distinguished from a
response to the recheck; this read-only identity evidence is not proof of a
fresh response to a settings change or of a working TCP callback.

Optionally request a short-lived JSON download. The redacted result is also
saved as the latest diagnostic attempt for a matching, unchanged device identity
and can be included in a subsequent Support Archive. It replaces the previous
command/Bluetooth diagnostic attempt; download older evidence first if needed.
Unknown reply text is not exported. Bluetooth addresses are omitted from the
shareable copy; collector identifiers use the existing masking. Private raw
result files remain local. An export error is shown separately from query results.
If PN could not be confirmed, the result is not attached to that collector's
Support Archive. The separate `.share.json` still remains under
`/config/eybond_local/diagnostic_runs/` after a successful export. Download or
attach that shareable copy when requested, not the raw `.json` or `.txt`.

### Free-form diagnostic commands

- `read`, `read_input`, and `sleep` do not write inverter settings.
- `write`, `write_single`, `write_bit`, free-form `ascii`, and `payload_hex`
  commands can change the device.
- Any scenario containing a potentially changing command is refused unless
  **Confirm commands that may change settings** is enabled.
- The scenario can temporarily override the command codec, addresses and wire
  format for that run. It does not rewrite the config entry or select a new
  permanent inverter profile.
- Only one diagnostic scenario runs for an entry at a time, using its current
  owned collector session.

Do not invent a write scenario or copy one intended for a different model.
Only run the exact commands supplied for your device by a developer you trust.

## How to run a scenario

1. Open **Settings → Devices & Services → EyeBond Local → Configure**.
2. Open **Diagnostics and service tools**.
3. Choose **Run diagnostic commands**.
4. Paste the complete multiline scenario.
5. Leave **Stop after the first error** enabled unless the developer says
   otherwise.
6. Enable the write confirmation only when the supplied scenario contains a
   deliberate write, ASCII or binary payload command.
7. Submit the form and wait for the result.

The same runner is available as the
`eybond_local.run_diagnostic_commands` action under **Developer Tools →
Actions**.

## Scenario format

Optional directives must appear before the first command:

| Directive | Meaning |
|---|---|
| `driver <key>` | Command codec: `modbus`, `modbus_smg`, `pi30`, `pi18`, `eybond_g_ascii`, or diagnostic-only `raw`. |
| `wire <auto\|fc4\|raw_modbus>` | Use normal routing (default), an EyeBond FC4 envelope, or bare Modbus RTU on the existing TCP stream. |
| `devcode <number>` | Override the run-scoped inverter device code. |
| `collector_addr <number>` | Override the collector-side address. |
| `device_addr <number>` | Override the inverter address. |
| `stop_on_error <true\|false>` | Stop or continue after a failed command. |
| `operation_timeout <seconds>` | Set the per-operation timeout for this run. |

Supported commands are:

| Command | Meaning |
|---|---|
| `read <register> [count]` | Read Modbus holding registers (FC03). |
| `read_input <register> [count]` | Read Modbus input registers (FC04). |
| `write <register> <value> [value ...]` | Write holding registers using FC16, even for one value. |
| `write_single <register> <value>` | Write exactly one holding register using FC06. |
| `write_bit <register> <bit> <0\|1>` | Change one bit in a register. |
| `ascii <command>` | Send one driver-supported ASCII command. |
| `payload_hex <hex bytes>` | Send exact binary payload bytes with `driver raw` and explicit `wire fc4`. |
| `sleep <milliseconds>` | Pause between commands. |

A read-only example for an already configured Modbus diagnostic target is:

```text
read 171 1
```

Driver and address overrides are intentionally not shown in a generic example;
they must match the exact collector and inverter protocol.

## Choosing the wire format

The command codec and the wire format are separate. `driver modbus` is a
model-independent diagnostic codec, **not** a new device profile. It can read
or write registers absent from the selected model's map. `modbus_smg` remains
an accepted compatibility name. Modbus register addresses are zero-based;
decimal and `0x` hexadecimal notation are accepted.

- `wire auto` preserves the existing transport's routing and negotiation.
- `wire fc4` explicitly wraps the payload in EyeBond FC4, including on an
  AT-primary connection that supports mixed frames. Both Modbus and the listed
  ASCII codecs are supported; this is not Modbus function code 4.
- `wire raw_modbus` sends bare Modbus RTU through the current collector socket.
  It requires `driver modbus` or `modbus_smg`, and a `device_addr` from 1 to 247.
  EyeBond `devcode` and `collector_addr` are unused and may be omitted.
  Broadcast writes are not supported.

Explicit overrides are available on the shared framed/AT collector transports;
an unsupported transport is rejected before sending anything. They **do not**
change the collector endpoint, UART settings, callback setup or learned wire
mode, and do not run a bootstrap command. A collector can therefore refuse the
requested format even if its cloud connection uses that format successfully.

The whole scenario is validated before its first request. An explicit override
is pinned to one physical connection: it cannot continue on a replacement
connection after a reconnect. Each explicit-wire exchange is attempted once,
with no implicit retry or fallback. Its default command timeout is 10 seconds,
with a maximum of 120 seconds. Normal `auto` read retry behavior is unchanged.

For bare RTU, a timeout, cancellation, invalid CRC, mismatched write echo or an
ambiguous frame closes that diagnostic socket. This prevents a late reply from
being attributed to a later command; wait for normal reconnection before a new
run. It does not restore or change an endpoint because none was changed. Short
RTU replies are matched by the explicitly selected slave, function, length and
CRC; this is an operator-selected protocol, not automatic detection.

Writes still require confirmation regardless of the selected format. A valid
write acknowledgement is **not** proof of the final setting: include a known
readback command when the device map provides one. This tool does not discover
valid value ranges or make an unknown write safe.

## Exact binary payloads

`payload_hex` is for developer-supplied protocols that cannot be expressed by
one of the supported codecs. Hex bytes may be contiguous or separated by spaces;
each byte must have two hexadecimal digits, without `0x` prefixes or escape
sequences. The payload must contain 1–512 bytes.

This requires explicit `driver raw`, `wire fc4`, `devcode`, `collector_addr` and
`device_addr` directives. No address is inherited from the active profile.
`device_addr` is recorded as context but is **not inserted into the payload**;
if the protocol needs an address, CRC or line ending, the supplied bytes must
already contain it. The collector FC4 envelope is added by the transport.

The entire payload is sent unchanged, once per command. There is no protocol
guessing, automatic decoding or fallback to a different wire. `raw` is only a
diagnostic codec, not a new inverter profile. It cannot be combined with
`read`, `write` or `ascii` commands in the same scenario.

Even a developer-provided read query needs write confirmation: EyeBond Local
cannot determine what arbitrary bytes do. A completed transport exchange is
not an inverter acknowledgement. Check `reply_status` (`payload_received` or
`empty_reply`) and `response_size`; `decode_status: not_attempted` means that
no telemetry, checksum or setting acknowledgement was validated.

## Results and downloads

The form shows the output from the latest run and the local result path.
The JSON result also includes the requested wire format and a bounded payload
trace: command line, route, request/reply bytes, sizes, timing and errors. The
trace stores at most 128 exchanges and 512 bytes per direction per exchange;
truncation and omitted exchanges are reported. These are payload bytes, not a
packet capture (the complete EyeBond header is not included). For `auto`, the requested
mode alone does not prove which wire format the transport ultimately used.

Explicit `fc4` and `raw_modbus` exchanges also include `transport_progress`:
the last local transport phase, whether `writer.write()` started/returned,
whether `drain()` completed on the owned connection, and whether a correlated
response was returned to the diagnostic operation.
FC4 includes its transaction ID (`tid`) once allocated, for comparison with
debug logs. This distinguishes a timeout waiting for a request/write lock from
a request written to the local socket while awaiting a response. Each exchange
has its own snapshot; cancellation and errors retain the last observed phase.

These fields are **not** a device-execution acknowledgement: `write_returned`
and `drain_completed` do not prove delivery to the collector or inverter.
`response_received` can be true for an empty or malformed FC4 payload; the
command's result still reports the decoding failure. A reply with an old TID
does not complete a newer request. These fields do not count unrelated or
buffered inbound traffic. Missing progress in an older export or an
unsupported transport is unknown, not proof that no bytes were sent.

If **Create shareable download link** is enabled, EyeBond Local creates a
redacted copy and a short-lived signed download URL. The URL is scoped to the
current entry and result. The unredacted local result is not exposed through
that download route or copied into `/config/www`.

Download the shareable result promptly and attach it to the relevant issue. A
normal Support Archive may still be requested because it contains the wider
runtime and device context.
Known identity ranges and recognizable identifiers are masked in the shareable
copy, including the new payload trace. Review the file before public sharing:
unknown register maps and opaque binary protocols can contain identifiers that
cannot be recognized reliably.

The latest completed diagnostic attempt (including a failed attempt) is also
saved as a redacted support attachment. A subsequent Support Archive includes
it when it is no more than seven days old and matches the same entry, collector
PN and known inverter identity/route. It survives a Home Assistant restart.
The archive records the capture's own timestamp and integration build; it does
not rerun the scenario. Results created before this feature do not have this
identity binding and are not automatically imported. The optional cache is
limited to 1 MiB per entry; an oversized or unbound newer attempt prevents an
older successful result from being presented as the latest capture.

For explicit FC4 diagnostics, the exported JSON also has a passive
`protocol_candidates` report. Initial descriptors cover vendor variants
`099E_06` and `09D5_01`, derived from static analysis of SmartClient 3.48.11.0.
They check the exact request, response length and additive checksum on the
original bytes, **before redaction**. The `099E` 72-byte length is a nominal
layout, not a vendor-enforced length check. A second, explicit 84-byte layout
matches the reply captured in [issue #82](https://github.com/groove-max/ha-eybond-local/issues/82#issuecomment-6686294107),
including its observed `VPHB` + `4F` prefix and checksum over the entire 78-byte
body. The report distinguishes the vendor parser's 66-byte body from the
12-byte extension; it does not truncate the reply or accept arbitrary lengths.
These are structural leads only: field meanings and general physical-device
compatibility remain unverified, and the extension has no decoded meaning yet.
No match does not rule out another firmware variant, and multiple matches
remain ambiguous. Redaction may prevent recomputing the checksum from the
shareable bytes. The report sends no requests, selects no driver and enables
no sensors or controls. A vendor protocol ID is not an FC4 envelope address.

### Experimental Manyi / 099E polling

For the investigated VPHB variant, the driver selector now offers
**EyeBond 099E / VPHB (experimental; read-only)**. Select it explicitly when
directed by support; Auto does not probe this protocol. An existing collector
connection is still required. Selecting this driver does not require changing
the endpoint, cloud protocol ID or UART settings.

The experimental profile reads grid/output voltage and frequency, battery
voltage and state of charge, load percentage, inverter/controller temperatures
and PV voltage. Each cycle uses one status query. There are no inverter write
controls, even in Full control mode, and no inferred PV power or energy totals.
The model is displayed as a protocol family, not an automatically identified
Manyi model. Values need comparison with the device display or corresponding
cloud sample; zero PV in the initial capture does not validate daytime PV.

Support Archives retain the raw status reply and unknown extension for further
analysis. As with other opaque captures, review the archive before sharing it.
This is experimental read support, not a complete or hardware-certified map.

## If a scenario fails

The JSON's `started_at` / `finished_at` describe scenario execution, not time
spent waiting for a running poll. `context.coordinator_timing` separately
records `runtime_lock_wait_ms` and `scenario_ms`. Debug logs also record the
wait for result export (which cannot be timed inside that same exported file).
Cloud-tool cleanup logs its wait for runtime ownership, including on unload.
These timings help distinguish a slow device from a busy polling channel or
slow storage; they do not cancel a poll or retry a command automatically.

Check that:

- the EyeBond Local entry is loaded;
- the collector is connected;
- inverter entities are not in the middle of a reconnect or protocol scan;
- the scenario was copied completely, with directives before commands;
- write confirmation was enabled only when required.

If the same developer-provided scenario still fails, report the visible error
and the shareable command result first. Create a Support Archive only if asked:
archive creation may perform additional reads when the inverter is unidentified.
