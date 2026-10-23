# Diagnostic Commands

Diagnostic commands are an advanced support tool for running a short,
developer-provided scenario through the collector connection that already
belongs to an EyeBond Local entry.

Most users never need this tool. Start with a [Support Archive](SUPPORT_ARCHIVE.md)
and use diagnostic commands only when a developer asks for a specific scenario.

## Safety boundary

- `read`, `read_input`, and `sleep` do not write inverter settings.
- `write`, `write_single`, `write_bit`, and free-form `ascii` commands can change the device.
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
   deliberate write or ASCII command.
7. Submit the form and wait for the result.

The same runner is available as the
`eybond_local.run_diagnostic_commands` action under **Developer Tools →
Actions**.

## Scenario format

Optional directives must appear before the first command:

| Directive | Meaning |
|---|---|
| `driver <key>` | Command codec: `modbus`, `modbus_smg`, `pi30`, `pi18`, or `eybond_g_ascii`. |
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

## Results and downloads

The form shows the output from the latest run and the local result path.
The JSON result also includes the requested wire format and a bounded payload
trace: command line, route, request/reply bytes, sizes, timing and errors. The
trace stores at most 128 exchanges and 512 bytes per direction per exchange;
truncation and omitted exchanges are reported. These are payload bytes, not a
packet capture (EyeBond headers/TIDs are not included). For `auto`, the requested
mode alone does not prove which wire format the transport ultimately used.

If **Create shareable download link** is enabled, EyeBond Local creates a
redacted copy and a short-lived signed download URL. The URL is scoped to the
current entry and result. The unredacted local result is not exposed through
that download route or copied into `/config/www`.

Download the shareable result promptly and attach it to the relevant issue. A
normal Support Archive may still be requested because it contains the wider
runtime and device context.
Known identity ranges and recognizable identifiers are masked in the shareable
copy, including the new payload trace. Review the file before public sharing:
unknown register maps can contain identifiers that cannot be recognized reliably.

## If a scenario fails

Check that:

- the EyeBond Local entry is loaded;
- the collector is connected;
- inverter entities are not in the middle of a reconnect or protocol scan;
- the scenario was copied completely, with directives before commands;
- write confirmation was enabled only when required.

If the same developer-provided scenario still fails, report the visible error
and the shareable command result first. Create a Support Archive only if asked:
archive creation may perform additional reads when the inverter is unidentified.
