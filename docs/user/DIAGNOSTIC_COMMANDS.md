# Diagnostic Commands

Diagnostic commands are an advanced support tool for running a short,
developer-provided scenario through the collector connection that already
belongs to an EyeBond Local entry.

Most users never need this tool. Start with a [Support Archive](SUPPORT_ARCHIVE.md)
and use diagnostic commands only when a developer asks for a specific scenario.

## Safety boundary

- `read` and `sleep` do not change inverter settings.
- `write`, `write_bit`, and free-form `ascii` commands can change the device.
- Any scenario containing a potentially changing command is refused unless
  **Confirm commands that may change settings** is enabled.
- The scenario can temporarily override the driver or wire addresses for that
  run, but it does not rewrite the config entry.
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
| `driver <key>` | Temporarily use a specific installed driver. |
| `devcode <number>` | Override the run-scoped inverter device code. |
| `collector_addr <number>` | Override the collector-side address. |
| `device_addr <number>` | Override the inverter address. |
| `stop_on_error <true\|false>` | Stop or continue after a failed command. |
| `operation_timeout <seconds>` | Set the per-operation timeout for this run. |

Supported commands are:

| Command | Meaning |
|---|---|
| `read <register> [count]` | Read one or more Modbus registers. |
| `write <register> <value> [value ...]` | Write one or more register values. |
| `write_bit <register> <bit> <0\|1>` | Change one bit in a register. |
| `ascii <command>` | Send one driver-supported ASCII command. |
| `sleep <milliseconds>` | Pause between commands. |

A harmless example that reads one register is:

```text
read 171 1
```

Driver and address overrides are intentionally not shown in a generic example;
they must match the exact collector and inverter protocol.

## Results and downloads

The form shows the output from the latest run and the local result path.

If **Create shareable download link** is enabled, EyeBond Local creates a
redacted copy and a short-lived signed download URL. The URL is scoped to the
current entry and result. The unredacted local result is not exposed through
that download route or copied into `/config/www`.

Download the shareable result promptly and attach it to the relevant issue. A
normal Support Archive may still be requested because it contains the wider
runtime and device context.

## If a scenario fails

Check that:

- the EyeBond Local entry is loaded;
- the collector is connected;
- inverter entities are not in the middle of a reconnect or protocol scan;
- the scenario was copied completely, with directives before commands;
- write confirmation was enabled only when required.

If the same developer-provided scenario still fails, create a fresh Support
Archive and report both the visible error and the command result.
