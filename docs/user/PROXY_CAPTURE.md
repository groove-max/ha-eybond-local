# Collector Proxy Capture

Proxy capture is an advanced support tool. Most users do not need it for normal monitoring or control.

Use it only when a developer asks you to collect a temporary capture from the collector.

## What it is for

Proxy capture records a short communication session so a developer can understand why a collector or inverter behaves differently from expected.

It can help when:

- the collector connects but the inverter is not identified;
- the vendor app or cloud sees data but Home Assistant does not;
- a model needs extra evidence before support can be added;
- a developer asks for a capture in a GitHub issue.

For most problems, start with **Create support archive** first. Proxy capture is the next step only when the archive is not enough.

## Before you start

Make sure:

- the collector has stable Wi-Fi;
- Home Assistant can already reach the collector;
- the collector uses the **Cloud + Home Assistant** operating profile;
- you are ready to keep the capture short;
- you can check that the vendor app still works afterward if you use one.

During the capture, Home Assistant relays the collector's cloud connection so
the vendor app should continue receiving data. A brief gap is still possible
while the temporary route starts, stops, or reconnects. Do not run a capture
during a critical monitoring or control window.

If you are not sure, stop and create a Support Archive instead.

**Read-only control mode also blocks collector changes**, including the temporary
server-address change needed for capture. If a developer has asked for a capture,
open **Configure → Polling and inverter detection** and select **Control mode →
Auto**. Leave the connection profile at **Cloud + Home Assistant**. Full Control
is not needed, and changing this setting does not itself send inverter commands.
After capture and confirmed restoration you can return to Read-only.

## How to start

1. **Settings → Devices & Services**
2. **EyeBond Local**
3. **Configure**
4. **Expand device support**
5. **Capture collector cloud traffic**
6. Choose the duration and start the capture.
7. Reproduce the problem, or follow the developer's instructions.
8. Stop the capture, or wait for the timer to finish.

Startup waits for any current inverter poll to finish, then pauses new polls
while it checks and redirects the connection. A failed start keeps polling
paused through its immediate restoration attempt. This prevents the integration
from sending a normal poll in the middle of preparation; it does not guarantee
that an unstable collector will stay connected.

If the last check found the collector disconnected but its route is known,
the screen offers **Reconnect and start capture**. This first reconnects to
the collector and reads its current server address, without needing to identify
the inverter. Only after the checks pass does Home Assistant temporarily change
the endpoint and start relaying traffic. If connection or address checks fail,
capture does not start and the endpoint is not changed. This is a retry option,
not a fix for an unstable collector connection.

**Start capture** runs the same connection and server-address checks. The label
reflects the latest known connection state; it is not a separate capture mode.
If reading the current server address times out, Home Assistant cannot safely
save the address for restoration and will not redirect the collector. Include
the error and a Support Archive in your report instead of changing the address
manually to get past the check.

Duration, start/stop, live status, and the saved result are all managed on this
screen. Older versions exposed separate proxy entities on the collector device;
those controls are obsolete and are removed during entity migration.

The screen shows any conditions that prevent capture from starting. If a capture
is already active or needs recovery, its status and recovery actions remain
available until Home Assistant finishes restoring the collector route.

<p align="center"><img src="../images/proxy-capture-running.png" alt="Running proxy capture session with timer and live log" width="720"></p>

## Timer behavior

Proxy capture is temporary.

When the timer ends, Home Assistant stops the capture automatically and tries to restore the collector’s normal connection path.

The open dialog does not refresh automatically. Choose **Refresh** or reopen it
to see the final status and saved result. Refresh reads the capture's current
status without requesting another inverter poll. It does not restart or extend
the timer; **Reset proxy timer** is a separate action. Check that the vendor app
resumes updating after restoration.

You can:

- stop the capture early;
- reset the timer if the developer asks for a longer capture;
- change the duration while the capture is running.

Refreshing the live log does not extend the timer.

## Downloading the result

After the capture finishes, the same screen shows a **Saved result** download
link. It uses an authenticated, short-lived API route rather than a public file
under `/local`.

<p align="center"><img src="../images/proxy-capture-result.png" alt="Finished proxy capture session with saved result download" width="720"></p>

Download the ZIP and attach it to the GitHub issue together with a short note about what you did during the capture.

If the developer also asks for a normal Support Archive, create it separately from **Configure → Diagnostics and service tools → Create support archive**.

## Restoring cloud/app access

Normally, EyeBond Local restores the collector automatically after capture.

If the vendor app stops showing live data afterward:

1. Open **Configure → Expand device support → Capture collector cloud traffic**.
2. Use the stop/recovery action shown for the current session.
3. Wait for Home Assistant to confirm restoration.
4. Check the vendor app again.

If the restore action is unavailable or the collector still does not recover, do not repeat captures. Create a Support Archive and report the issue.
